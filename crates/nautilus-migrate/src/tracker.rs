use crate::ddl::DatabaseProvider;
use crate::error::{MigrationError, Result};
use crate::migration::{Migration, MigrationRecord};
use chrono::Utc;
use sqlx::{AnyPool, Row};
use std::sync::Arc;

/// Tracks applied migrations in the database
pub struct MigrationTracker {
    pool: Arc<AnyPool>,
    provider: DatabaseProvider,
}

impl MigrationTracker {
    /// Create a new migration tracker
    pub fn new(pool: Arc<AnyPool>, provider: DatabaseProvider) -> Self {
        Self { pool, provider }
    }

    /// Initialize the migration tracking table
    pub async fn init(&self) -> Result<()> {
        let create_table_sql = self.create_migrations_table_sql();

        sqlx::query(&create_table_sql)
            .persistent(false)
            .execute(self.pool.as_ref())
            .await?;

        Ok(())
    }

    /// Generate SQL to create the migrations tracking table
    fn create_migrations_table_sql(&self) -> String {
        match self.provider {
            DatabaseProvider::Postgres => r#"
CREATE TABLE IF NOT EXISTS _nautilus_migrations (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    checksum TEXT NOT NULL,
    applied_at TEXT NOT NULL,
    execution_time_ms BIGINT NOT NULL
)
            "#
            .trim()
            .to_string(),
            DatabaseProvider::Mysql => r#"
CREATE TABLE IF NOT EXISTS _nautilus_migrations (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(500) NOT NULL UNIQUE,
    checksum VARCHAR(500) NOT NULL,
    applied_at VARCHAR(100) NOT NULL,
    execution_time_ms BIGINT NOT NULL
)
            "#
            .trim()
            .to_string(),
            DatabaseProvider::Sqlite => r#"
CREATE TABLE IF NOT EXISTS _nautilus_migrations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    checksum TEXT NOT NULL,
    applied_at TEXT NOT NULL,
    execution_time_ms INTEGER NOT NULL
)
            "#
            .trim()
            .to_string(),
        }
    }

    /// Get all applied migrations
    pub async fn get_applied_migrations(&self) -> Result<Vec<MigrationRecord>> {
        let rows = sqlx::query(
            "SELECT name, checksum, applied_at, execution_time_ms FROM _nautilus_migrations ORDER BY id"
        )
        .persistent(false)
        .fetch_all(self.pool.as_ref())
        .await?;

        let mut records = Vec::new();
        for row in rows {
            let name: String = row.try_get("name")?;
            let checksum: String = row.try_get("checksum")?;
            let applied_at_str: String = row.try_get("applied_at")?;
            let execution_time_ms: i64 = row.try_get("execution_time_ms")?;

            let applied_at = applied_at_str
                .parse()
                .map_err(|e| MigrationError::Other(format!("Invalid timestamp: {}", e)))?;

            records.push(MigrationRecord {
                name,
                checksum,
                applied_at,
                execution_time_ms,
            });
        }

        Ok(records)
    }

    /// Check if a migration has been applied
    pub async fn is_applied(&self, name: &str) -> Result<bool> {
        let row = sqlx::query("SELECT COUNT(*) as count FROM _nautilus_migrations WHERE name = ?")
            .persistent(false)
            .bind(name)
            .fetch_one(self.pool.as_ref())
            .await?;

        let count: i64 = row.try_get("count")?;
        Ok(count > 0)
    }

    /// Record that a migration was applied
    pub async fn record_migration(
        &self,
        migration: &Migration,
        execution_time_ms: i64,
    ) -> Result<()> {
        let applied_at = Utc::now().to_rfc3339();

        sqlx::query(
            "INSERT INTO _nautilus_migrations (name, checksum, applied_at, execution_time_ms) VALUES (?, ?, ?, ?)"
        )
        .persistent(false)
        .bind(&migration.name)
        .bind(&migration.checksum)
        .bind(&applied_at)
        .bind(execution_time_ms)
        .execute(self.pool.as_ref())
        .await?;

        Ok(())
    }

    /// Record that a migration was applied (within a transaction)
    pub async fn record_migration_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Any>,
        migration: &Migration,
        execution_time_ms: i64,
    ) -> Result<()> {
        let applied_at = Utc::now().to_rfc3339();

        sqlx::query(
            "INSERT INTO _nautilus_migrations (name, checksum, applied_at, execution_time_ms) VALUES (?, ?, ?, ?)"
        )
        .persistent(false)
        .bind(&migration.name)
        .bind(&migration.checksum)
        .bind(&applied_at)
        .bind(execution_time_ms)
        .execute(&mut **tx)
        .await?;

        Ok(())
    }

    /// Remove a migration record (for rollback)
    pub async fn remove_migration(&self, name: &str) -> Result<()> {
        sqlx::query("DELETE FROM _nautilus_migrations WHERE name = ?")
            .persistent(false)
            .bind(name)
            .execute(self.pool.as_ref())
            .await?;

        Ok(())
    }

    /// Remove a migration record (within a transaction)
    pub async fn remove_migration_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Any>,
        name: &str,
    ) -> Result<()> {
        sqlx::query("DELETE FROM _nautilus_migrations WHERE name = ?")
            .persistent(false)
            .bind(name)
            .execute(&mut **tx)
            .await?;

        Ok(())
    }

    /// Verify that all applied migrations match their checksums
    pub async fn verify_migrations(&self, migrations: &[Migration]) -> Result<()> {
        let applied = self.get_applied_migrations().await?;

        for record in applied {
            if let Some(migration) = migrations.iter().find(|m| m.name == record.name) {
                if migration.checksum != record.checksum {
                    return Err(MigrationError::ChecksumMismatch {
                        name: record.name,
                        expected: record.checksum,
                        found: migration.checksum.clone(),
                    });
                }
            } else {
                return Err(MigrationError::NotFound(format!(
                    "Applied migration '{}' not found in migration files",
                    record.name
                )));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore = "AnyPool::connect requires runtime configuration"]
    async fn test_migrations_table_sql() {
        let pool = AnyPool::connect("sqlite::memory:").await.unwrap();

        let tracker = MigrationTracker::new(Arc::new(pool), DatabaseProvider::Sqlite);
        tracker.init().await.unwrap();

        let applied = tracker.get_applied_migrations().await.unwrap();
        assert_eq!(applied.len(), 0);
    }
}
