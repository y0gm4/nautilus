# Nautilus Schema Language Grammar

This document defines the complete grammar for the schema language used in Nautilus. The grammar is specified in Extended Backus-Naur Form (EBNF).

## Notation

- `::=` means "is defined as"
- `|` separates alternatives
- `[ ... ]` indicates optional elements (zero or one)
- `{ ... }` indicates repetition (zero or more)
- `( ... )` groups elements
- `'...'` denotes literal terminals (keywords, operators)
- `"..."` denotes string literals
- `Ident`, `String`, `Number` are terminal tokens from the lexer

## Schema Structure

### Top-Level

```ebnf
Schema ::= Declaration* EOF

Declaration ::= DatasourceDecl
              | GeneratorDecl  
              | ModelDecl
              | TypeDecl
              | EnumDecl

Newline ::= '\n' | '\r\n'
```

## Declarations

### Datasource

```ebnf
DatasourceDecl ::= 'datasource' Ident '{' Newline*
                   ConfigField*
                   '}' Newline*

ConfigField ::= Ident '=' Expr Newline*
```

**Example:**
```prisma
datasource db {
  provider = "postgresql"
  url      = env("DATABASE_URL")
}
```

### Generator

```ebnf
GeneratorDecl ::= 'generator' Ident '{' Newline*
                  ConfigField*
                  '}' Newline*
```

**Fields:**

| Field | Required | Values | Default | Description |
|-------|----------|--------|---------|-------------|
| `provider` | ✓ | `"nautilus-client-rs"` \| `"nautilus-client-py"` \| `"nautilus-client-js"` | — | Code-generation target language |
| `output` | — | string path | — | Output directory for generated files |
| `interface` | — | `"sync"` \| `"async"` | `"sync"` | Whether to generate a synchronous or asynchronous client API |
| `recursive_type_depth` | — | positive integer | `5` | Depth of recursive include TypedDicts (Python client only) |

The `interface` fields:
- `"sync"` (default): generates plain `fn` / `def` methods. Rust delegates use
  `tokio::task::block_in_place`; Python delegates use `asyncio.run()`.
- `"async"`: generates `async fn` / `async def` methods with `.await` / `await`
  at every call-site.

The `recursive_type_depth` field controls how many levels of nested `include` TypedDicts
are generated for the Python client:
- Each depth level adds a `{Model}IncludeRecursive{N}` type and the corresponding
  `FindMany{Target}ArgsFrom{Source}Recursive{N}` types.
- At the maximum depth the `include` field is omitted, preventing infinite type recursion.
- Minimum accepted value is `1`. Values of `0` or below are treated as `1`.

**Example:**
```prisma
generator client {
  provider  = "nautilus-client-rs"
  output    = "../generated"
  interface = "async"
}
```

```prisma
generator client {
  provider            = "nautilus-client-py"
  output              = "../generated"
  interface           = "async"
  recursive_type_depth  = 3   # default is 5
}
```

Java also supports a dedicated generator shape:

```prisma
generator client {
  provider    = "nautilus-client-java"
  output      = "../generated-java"
  package     = "com.acme.db"
  group_id    = "com.acme"
  artifact_id = "db-client"
  mode        = "jar"
  interface   = "async"
}
```

The Java-only fields are `package`, `group_id`, `artifact_id`, and `mode`.

### Model

```ebnf
ModelDecl ::= 'model' Ident '{' Newline*
              ( FieldDecl | ModelAttribute Newline* )*
              '}' Newline*

FieldDecl ::= Ident FieldType FieldModifier? FieldAttribute* Newline*

FieldModifier ::= '?' | '!' | '[' ']'

ModelAttribute ::= '@@' AttributeName AttributeArgs?
```

**Example:**
```prisma
model User {
  id        Int      @id @default(autoincrement())
  email     String!  @unique
  posts     Post[]
  profile   Profile?

  @@map("users")
  @@index([email])
}
```

### Composite Type

```ebnf
TypeDecl ::= 'type' Ident '{' Newline*
             FieldDecl*
             '}' Newline*
```

Composite `type` blocks define reusable embedded structures.

```prisma
type Address {
  street String
  city   String
  kind   AddressKind
}
```

**Constraints:**
- Fields inside `type` blocks may be scalar or enum types
- Nested composite types and model relations are not allowed
- Only `@map` and `@store(...)` are allowed on composite-type fields

### Enum

```ebnf
EnumDecl ::= 'enum' Ident '{' Newline*
             EnumVariant*
             '}' Newline*

EnumVariant ::= Ident Newline*
```

**Example:**
```prisma
enum Role {
  USER
  ADMIN
  MODERATOR
}
```

## Types

### Field Types

```ebnf
FieldType ::= ScalarType
            | DecimalType
            | UserType

ScalarType ::= 'String'
             | 'Boolean'
             | 'Int'
             | 'BigInt'
             | 'Float'
             | 'DateTime'
             | 'Bytes'
             | 'Json'
             | 'Jsonb'
             | 'Uuid'
             | 'Xml'
             | 'Char' '(' Number ')'
             | 'VarChar' '(' Number ')'

DecimalType ::= 'Decimal' '(' Number ',' Number ')'

UserType ::= Ident  // Reference to model or enum
```

**Examples:**
```prisma
field1  String        // Scalar type (implicitly NOT NULL)
field2  Decimal(10,2) // Decimal with precision and scale
field3  Role          // User-defined enum
field4  Post          // User-defined model
field5  String?       // Optional scalar (nullable)
field6  String!       // Explicitly NOT NULL scalar
field7  Post[]        // Array of models
field8  Jsonb         // PostgreSQL-only JSONB
field9  VarChar(255)  // Bounded string
```

## Attributes

### Field Attributes

```ebnf
FieldAttribute ::= '@' AttributeName AttributeArgs?

AttributeName ::= 'id'
                | 'unique'
                | 'default'
                | 'map'
                | 'store'
                | 'relation'
                | 'updatedAt'
                | 'computed'
                | 'check'

AttributeArgs ::= '(' ArgumentList? ')'

ArgumentList ::= Argument ( ',' Argument )*

Argument ::= Expr                    // Positional argument
           | Ident ':' Expr          // Named argument

ComputedKind ::= 'Stored' | 'Virtual'

RawExpr ::= Token+   // Raw SQL tokens, parsed until top-level comma
```

**Recognized Field Attributes:**

#### @id
Marks field as primary key.

```prisma
id Int @id
```

#### @unique
Adds unique constraint.

```prisma
email String @unique
```

#### @default(expr)
Specifies default value.

```prisma
createdAt DateTime @default(now())
count     Int      @default(0)
id        Int      @default(autoincrement())
uuid      Uuid     @default(uuid())
role      String   @default("USER")
active    Boolean  @default(true)
```

#### @map("name")
Maps to physical database column name.

```prisma
userId Int @map("user_id")
```

#### @store(json | native)
Controls how array and composite-type fields are stored when provider capabilities differ.

```prisma
tags    String[] @store(json)
profile Address  @store(json)
```

- `json` — serialize into a JSON column/value
- `native` — use the provider's native array/composite representation when supported

#### @updatedAt
Marks a `DateTime` field to be automatically set to the current timestamp on every CREATE and UPDATE operation. The framework manages this value — it is excluded from all user-input types.

```prisma
updatedAt DateTime @updatedAt
```

#### @computed(expr, Stored | Virtual)
Declares a database-generated (computed) column. The expression is raw SQL evaluated by the database engine.

```prisma
total     Int    @computed(price * quantity, Stored)
fullName  String @computed(first_name || ' ' || last_name, Virtual)
```

- `Stored` — value is computed on write and persisted physically on disk
- `Virtual` — value is computed on read and never stored (not supported on PostgreSQL)

Maps to SQL:
- **PostgreSQL**: `GENERATED ALWAYS AS (expr) STORED`
- **MySQL**: `GENERATED ALWAYS AS (expr) STORED|VIRTUAL`
- **SQLite**: `AS (expr) STORED|VIRTUAL`

**Constraints:**
- Cannot be combined with `@id`, `@default`, or `@updatedAt`
- Cannot be applied to array (`[]`) or relation fields
- `Virtual` is a validation error when the datasource provider is `postgresql`
- Computed fields are read-only — excluded from all create/update input types

#### @check(expr)
Adds a column-level SQL `CHECK` constraint.

```prisma
age    Int    @check(age >= 0 AND age <= 150)
status Status @check(status IN [ACTIVE, PENDING])
```

**Constraints:**
- Field-level `@check` may only reference the decorated field itself
- It cannot be applied to relation, array, or computed fields

#### @relation(...)
Defines relationship with named arguments. The `name` parameter is optional but required when multiple relations exist between the same models.

```prisma
author User @relation(
  name: "AuthoredPosts",
  fields: [authorId],
  references: [id],
  onDelete: Cascade,
  onUpdate: Restrict
)
```

**Supported parameters:**
- `name` (optional): Unique identifier for the relation, required when multiple relations exist between the same two models
- `fields`: Array of field names in the current model that form the foreign key
- `references`: Array of field names in the referenced model (must be primary key or unique)
- `onDelete` (optional): Referential action on delete
- `onUpdate` (optional): Referential action on update

### Model Attributes

```ebnf
ModelAttribute ::= '@@' AttributeName AttributeArgs?

AttributeName ::= 'map'
                | 'id'
                | 'unique'
                | 'index'
                | 'check'

IndexArgs ::= IdentArray ( ',' IndexNamedArg )*
IndexNamedArg ::= 'type' ':' IndexType
               | 'name' ':' String
               | 'map'  ':' String
IndexType ::= 'BTree' | 'Hash' | 'Gin' | 'Gist' | 'Brin' | 'FullText'
```

**Recognized Model Attributes:**

#### @@map("name")
Maps to physical database table name.

```prisma
model User {
  id Int @id
  @@map("users")
}
```

#### @@id([field1, field2, ...])
Composite primary key.

```prisma
model UserRole {
  userId Int
  roleId Int
  
  @@id([userId, roleId])
}
```

#### @@unique([field1, field2, ...])
Composite unique constraint.

```prisma
model User {
  email    String
  username String
  
  @@unique([email, username])
}
```

#### @@index([field1, field2, ...], type?, name?, map?)
Database index. Supports optional named arguments:

| Argument | Type | Description |
|---|---|---|
| `type` | Ident | Index access method (see table below). Omit to let the DBMS choose (BTree). |
| `name` | String | Logical developer name (not used in DDL). |
| `map` | String | Physical DDL index name override (default: `idx_{table}_{cols}`). |

**Supported index types by database:**

| Type | PostgreSQL | MySQL | SQLite |
|---|:---:|:---:|:---:|
| `BTree` (default) | ✅ | ✅ | ✅ |
| `Hash` | ✅ | ✅ (8+) | ❌ |
| `Gin` | ✅ | ❌ | ❌ |
| `Gist` | ✅ | ❌ | ❌ |
| `Brin` | ✅ | ❌ | ❌ |
| `FullText` | ❌ | ✅ | ❌ |

Using an unsupported type for the declared datasource provider is a **validation error**.

```prisma
model Post {
  authorId  Int
  createdAt DateTime
  content   String
  
  @@index([authorId, createdAt])
  @@index([authorId, createdAt], type: BTree, map: "idx_post_author_date")
  @@index([content], type: Gin)
}
```

#### @@check(expr)
Adds a table-level SQL `CHECK` constraint.

```prisma
model Booking {
  startDate DateTime
  endDate   DateTime

  @@check(startDate < endDate)
}
```

Unlike field-level `@check`, the model-level form may reference multiple scalar fields.

## Expressions

```ebnf
Expr ::= Literal
       | FunctionCall
       | Array
       | Ident

Literal ::= String
          | Number
          | Boolean

Boolean ::= 'true' | 'false'

FunctionCall ::= Ident '(' ArgumentList? ')'

Array ::= '[' ( Expr ( ',' Expr )* )? ']'
```

**Examples:**
```prisma
"string literal"           // String
42                         // Number
3.14                       // Number
true                       // Boolean
false                      // Boolean
autoincrement()            // Function call
uuid()                     // Function call
now()                      // Function call
env("DATABASE_URL")        // Function call with argument
[userId]                   // Array with single element
[email, username]          // Array with multiple elements
```

### Named Arguments

Used in `@relation` and potentially other attributes:

```ebnf
NamedArg ::= Ident ':' Expr
```

**Example:**
```prisma
@relation(
  name: "PostAuthor",
  fields: [userId],
  references: [id],
  onDelete: Cascade
)
```

## Referential Actions

Used with `@relation` for foreign key constraints:

```ebnf
ReferentialAction ::= 'Cascade'
                    | 'Restrict'
                    | 'NoAction'
                    | 'SetNull'
                    | 'SetDefault'
```

**Example:**
```prisma
user User @relation(
  fields: [userId],
  references: [id],
  onDelete: Cascade,
  onUpdate: SetNull
)
```

## Lexical Grammar

### Tokens

Reference: See [`token.rs`](src/token.rs) for complete token definitions.

```ebnf
Token ::= Keyword
        | Ident
        | String
        | Number
        | Punctuation
        | Attribute
        | Newline
        | EOF

Keyword ::= 'datasource' | 'generator' | 'model' | 'enum'
          | 'true' | 'false'

Ident ::= [a-zA-Z_][a-zA-Z0-9_]*

String ::= '"' StringChar* '"'
StringChar ::= [^"\\\n] | EscapeSeq
EscapeSeq ::= '\\' ( '"' | 'n' | 't' | 'r' | '\\' )

Number ::= Digit+ ( '.' Digit+ )?
Digit ::= [0-9]

Punctuation ::= '{' | '}' | '[' | ']' | '(' | ')'
              | ',' | ':' | '=' | '?' | '!'
              | '*' | '+' | '-' | '/' | '%'
              | '<' | '>' | '|' | '||'

Attribute ::= '@' | '@@'

Comment ::= LineComment | BlockComment
LineComment ::= '//' [^\n]* '\n'
BlockComment ::= '/*' ( [^*] | '*' [^/] )* '*/'
```

### Whitespace

Whitespace (spaces, tabs) is ignored except for newlines, which are significant for statement termination.

### Comments

Both single-line (`//`) and multi-line (`/* */`) comments are supported and ignored by the parser.

## AST Mapping

Each grammar production maps to an AST node type defined in [`ast.rs`](src/ast.rs):

| Grammar Production | AST Type |
|-------------------|----------|
| `Schema` | [`Schema`](src/ast.rs) |
| `Declaration` | [`Declaration`](src/ast.rs) enum |
| `DatasourceDecl` | [`DatasourceDecl`](src/ast.rs) |
| `GeneratorDecl` | [`GeneratorDecl`](src/ast.rs) |
| `ModelDecl` | [`ModelDecl`](src/ast.rs) |
| `EnumDecl` | [`EnumDecl`](src/ast.rs) |
| `FieldDecl` | [`FieldDecl`](src/ast.rs) |
| `FieldType` | [`FieldType`](src/ast.rs) enum |
| `FieldAttribute` | [`FieldAttribute`](src/ast.rs) enum |
| `ModelAttribute` | [`ModelAttribute`](src/ast.rs) enum |
| `Expr` | [`Expr`](src/ast.rs) enum |
| `Literal` | [`Literal`](src/ast.rs) enum |

## Visitor Pattern

The AST supports traversal via the Visitor pattern. See [`visitor.rs`](src/visitor.rs) for details.

**Example visitor implementation:**

```rust
use nautilus_schema::visitor::{Visitor, walk_model};
use nautilus_schema::ast::*;
use nautilus_schema::Result;

struct ModelCounter {
    count: usize,
}

impl Visitor for ModelCounter {
    fn visit_model(&mut self, model: &ModelDecl) -> Result<()> {
        self.count += 1;
        walk_model(self, model) // Continue traversing
    }
}
```

## Grammar Ambiguities and Precedence

### Statement Termination

Newlines are used to terminate field declarations and configuration fields. Multiple newlines are allowed and ignored.

### Optional vs. Required vs. Not-Null

- Fields without any modifier are implicitly required (NOT NULL in SQL)
- Fields with `!` are **explicitly** NOT NULL — identical SQL/codegen behaviour to no modifier, but self-documenting
- Fields with `?` are optional (nullable — no NOT NULL constraint in SQL, wrapped in `Option<T>` / `T | null`)
- Fields with `[]` are arrays (one-to-many relations or lists)
- `!` cannot be used on relation fields (NOT NULL is a column-level constraint and relations have no column)

### User Types vs. Keywords

Field type names like `String`, `Int`, etc., are treated as keywords in type position. Other identifiers in type position are treated as references to user-defined models or enums.

## Error Recovery

The parser implements error recovery at declaration boundaries. If a parse error occurs within a declaration, the parser will:

1. Report the error
2. Skip tokens until the next declaration keyword (`datasource`, `generator`, `model`, `enum`)
3. Continue parsing

This allows multiple errors to be reported in a single parse run.

## Examples

### Complete Schema Example

```prisma
datasource db {
  provider = "postgresql"
  url      = env("DATABASE_URL")
}

generator client {
  provider = "nautilus-client-rs"
  output   = "../generated"
}

enum Role {
  USER
  ADMIN
}

model User {
  id        Uuid     @id @default(uuid()) @map("user_id")
  email     String   @unique
  role      Role     @default(USER)
  createdAt DateTime @default(now()) @map("created_at")
  
  posts     Post[]
  
  @@map("users")
}

model Post {
  id        BigInt   @id @default(autoincrement())
  userId    Uuid     @map("user_id")
  title     String
  rating    Decimal(10, 2)
  published Boolean  @default(false)
  createdAt DateTime @default(now())
  
  user      User     @relation(
    fields: [userId],
    references: [id],
    onUpdate: Cascade,
    onDelete: Cascade
  )
  
  @@map("posts")
  @@index([userId, createdAt])
}
```

## Validation

This grammar specifies only **syntax**. Semantic validation (checking that referenced models exist, types are valid, etc.) is performed in Phase 9.1.4 and is not part of the parser.

The parser produces a syntax-valid AST even if the schema has semantic errors like:
- References to non-existent models
- Invalid default values for field types
- Circular dependencies
- Missing required attributes

## Implementation Notes

The parser is implemented as a recursive descent parser in [`parser.rs`](src/parser.rs). Key features:

- **One-token lookahead**: Uses `peek()` to make parsing decisions
- **Error recovery**: Attempts to continue after errors
- **Span tracking**: Every AST node includes source location
- **No left recursion**: Grammar is designed to avoid left recursion
- **No backtracking**: Predictive parsing with single lookahead

## Future Extensions

Grammar features planned for future phases:

- View declarations
- Function declarations
- Trigger definitions
- Advanced constraint syntax
- Custom type definitions
- Native database types (`@db.VarChar(255)`)

---

**Parser Implementation**: [`parser.rs`](src/parser.rs)  
**AST Definitions**: [`ast.rs`](src/ast.rs)  
**Visitor Pattern**: [`visitor.rs`](src/visitor.rs)  
**Lexer Implementation**: [`lexer.rs`](src/lexer.rs)  
**Token Definitions**: [`token.rs`](src/token.rs)
