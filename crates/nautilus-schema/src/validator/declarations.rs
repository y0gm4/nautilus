use super::*;

impl SchemaValidator<'_> {
    pub(super) fn validate_datasources(&mut self) {
        let datasources: Vec<_> = self
            .schema
            .declarations
            .iter()
            .filter_map(|decl| match decl {
                Declaration::Datasource(datasource) => Some(datasource.clone()),
                _ => None,
            })
            .collect();

        for datasource in &datasources {
            self.validate_datasource(datasource);
        }
    }

    pub(super) fn validate_datasource(&mut self, datasource: &DatasourceDecl) {
        for field in &datasource.fields {
            if !KNOWN_DATASOURCE_FIELDS.contains(&field.name.value.as_str()) {
                self.errors.push_back(SchemaError::Validation(
                    format!(
                        "Unknown field '{}' in datasource block. Valid fields: {}",
                        field.name.value,
                        KNOWN_DATASOURCE_FIELDS.join(", ")
                    ),
                    field.span,
                ));
            }
        }

        if let Err(err) = Self::datasource_provider_value(datasource) {
            self.errors.push_back(err);
        }

        if let Err(err) = Self::datasource_url_value(datasource) {
            self.errors.push_back(err);
        }

        if let Err(err) = Self::datasource_direct_url_value(datasource) {
            self.errors.push_back(err);
        }

        self.validate_datasource_extensions(datasource);
        self.validate_datasource_preserve_extensions(datasource);
    }

    pub(super) fn validate_datasource_extensions(&mut self, datasource: &DatasourceDecl) {
        let Some(field) = datasource.find_field("extensions") else {
            return;
        };

        let provider_is_postgres = Self::datasource_provider_value(datasource)
            .ok()
            .and_then(|p| p.parse::<DatabaseProvider>().ok())
            .is_some_and(|p| p == DatabaseProvider::Postgres);

        if !provider_is_postgres {
            self.errors.push_back(SchemaError::Validation(
                "Datasource field 'extensions' is only supported for the \
                 'postgresql' provider"
                    .to_string(),
                field.span,
            ));
            return;
        }

        let Expr::Array { elements, .. } = &field.value else {
            self.errors.push_back(SchemaError::Validation(
                "Datasource 'extensions' must be an array of identifiers or \
                 string literals (e.g. [pg_trgm, \"uuid-ossp\"])"
                    .to_string(),
                field.span,
            ));
            return;
        };

        let mut seen: HashSet<String> = HashSet::new();
        for element in elements {
            let parsed = Self::parse_extension_entry(element);
            let (name, span) = match parsed {
                Ok(entry) => (entry.name, entry.span),
                Err(err) => {
                    self.errors.push_back(err);
                    continue;
                }
            };

            let normalized = name.to_lowercase();
            if normalized.is_empty() {
                self.errors.push_back(SchemaError::Validation(
                    "Extension name must not be empty".to_string(),
                    span,
                ));
                continue;
            }

            if !seen.insert(normalized.clone()) {
                self.errors.push_back(SchemaError::Validation(
                    format!("Duplicate extension '{}' in datasource", normalized),
                    span,
                ));
                continue;
            }

            if !KNOWN_POSTGRES_EXTENSIONS.contains(&normalized.as_str()) {
                self.warnings.push_back(SchemaError::Warning(
                    format!(
                        "Extension '{}' is not in Nautilus' curated list of \
                         supported PostgreSQL extensions. It will still be \
                         installed via CREATE EXTENSION IF NOT EXISTS, but \
                         Nautilus has not verified its availability",
                        normalized
                    ),
                    span,
                ));
            }
        }
    }

    /// Parse a single `extensions = [...]` array entry into a (name, span)
    /// pair. Accepts three forms:
    ///
    /// - `pg_trgm` (bare identifier)
    /// - `"uuid-ossp"` (quoted string literal)
    /// - `extension(name = vector, schema = "extensions")` (structured)
    pub(super) fn parse_extension_entry(expr: &Expr) -> Result<ParsedExtensionEntry> {
        match expr {
            Expr::Ident(ident) => Ok(ParsedExtensionEntry {
                name: ident.value.clone(),
                schema: None,
                span: ident.span,
            }),
            Expr::Literal(Literal::String(s, span)) => Ok(ParsedExtensionEntry {
                name: s.clone(),
                schema: None,
                span: *span,
            }),
            Expr::FunctionCall { name, args, span } if name.value == "extension" => {
                let mut ext_name: Option<String> = None;
                let mut schema: Option<String> = None;
                let mut saw_positional = false;

                for (idx, arg) in args.iter().enumerate() {
                    match arg {
                        Expr::NamedArg {
                            name: arg_name,
                            value,
                            span: arg_span,
                        } => match arg_name.value.as_str() {
                            "name" => {
                                ext_name = Some(extract_extension_string(value, *arg_span)?);
                            }
                            "schema" => {
                                schema = Some(extract_extension_string(value, *arg_span)?);
                            }
                            other => {
                                return Err(SchemaError::Validation(
                                    format!(
                                        "Unknown 'extension(...)' argument '{}'. \
                                         Supported: name, schema",
                                        other
                                    ),
                                    *arg_span,
                                ));
                            }
                        },
                        _ if idx == 0 && !saw_positional => {
                            saw_positional = true;
                            ext_name = Some(extract_extension_string(arg, arg.span())?);
                        }
                        _ => {
                            return Err(SchemaError::Validation(
                                "'extension(...)' arguments after the first must \
                                 be named (e.g. schema = \"extensions\")"
                                    .to_string(),
                                arg.span(),
                            ));
                        }
                    }
                }

                let Some(name) = ext_name else {
                    return Err(SchemaError::Validation(
                        "'extension(...)' requires a 'name' argument".to_string(),
                        *span,
                    ));
                };

                Ok(ParsedExtensionEntry {
                    name,
                    schema,
                    span: *span,
                })
            }
            Expr::FunctionCall { name, span, .. } => Err(SchemaError::Validation(
                format!(
                    "Unsupported extension entry '{}(...)'. Use an identifier, \
                     a string literal, or the 'extension(name = ..., schema = ...)' form",
                    name.value
                ),
                *span,
            )),
            other => Err(SchemaError::Validation(
                "Extension entries must be identifiers, string literals, or \
                 'extension(name = ..., schema = ...)' calls"
                    .to_string(),
                other.span(),
            )),
        }
    }

    pub(super) fn validate_datasource_preserve_extensions(&mut self, datasource: &DatasourceDecl) {
        let Some(field) = datasource.find_field("preserve_extensions") else {
            return;
        };

        let provider_is_postgres = Self::datasource_provider_value(datasource)
            .ok()
            .and_then(|p| p.parse::<DatabaseProvider>().ok())
            .is_some_and(|p| p == DatabaseProvider::Postgres);

        if !provider_is_postgres {
            self.errors.push_back(SchemaError::Validation(
                "Datasource field 'preserve_extensions' is only supported for the \
                 'postgresql' provider"
                    .to_string(),
                field.span,
            ));
            return;
        }

        if !matches!(field.value, Expr::Literal(Literal::Boolean(_, _))) {
            self.errors.push_back(SchemaError::Validation(
                "Datasource 'preserve_extensions' must be a boolean literal".to_string(),
                field.span,
            ));
        }
    }

    pub(super) fn validate_generators(&mut self) {
        let generators: Vec<_> = self
            .schema
            .declarations
            .iter()
            .filter_map(|decl| match decl {
                Declaration::Generator(generator) => Some(generator.clone()),
                _ => None,
            })
            .collect();

        for generator in &generators {
            self.validate_generator(generator);
        }
    }

    pub(super) fn validate_generator(&mut self, generator: &GeneratorDecl) {
        let provider_info = match Self::generator_provider_info(generator) {
            Ok(info) => Some(info),
            Err(err) => {
                self.errors.push_back(err);
                None
            }
        };

        if let Err(err) = Self::generator_output_value(generator) {
            self.errors.push_back(err);
        }

        if let Err(err) = Self::generator_interface_kind(generator) {
            self.errors.push_back(err);
        }

        if let Some((provider, client_provider)) = provider_info.as_ref() {
            if let Err(err) =
                Self::generator_recursive_type_depth(generator, *client_provider, provider)
            {
                self.errors.push_back(err);
            }
            if let Err(err) = Self::generator_java_output_required(generator, *client_provider) {
                self.errors.push_back(err);
            }
            if let Err(err) = Self::generator_java_package_value(generator, *client_provider) {
                self.errors.push_back(err);
            }
            if let Err(err) = Self::generator_java_group_id_value(generator, *client_provider) {
                self.errors.push_back(err);
            }
            if let Err(err) = Self::generator_java_artifact_id_value(generator, *client_provider) {
                self.errors.push_back(err);
            }
            if let Err(err) = Self::generator_java_mode_value(generator, *client_provider) {
                self.errors.push_back(err);
            }
        }

        let valid_fields = Self::valid_generator_fields(provider_info.as_ref().map(|(_, p)| *p));
        let valid_fields_text = valid_fields.join(", ");

        for field in &generator.fields {
            if valid_fields.contains(&field.name.value.as_str()) {
                continue;
            }

            let message = if let Some((provider, _)) = provider_info.as_ref() {
                format!(
                    "Unknown field '{}' in generator block. Valid fields for '{}': {}",
                    field.name.value, provider, valid_fields_text
                )
            } else {
                format!(
                    "Unknown field '{}' in generator block. Valid fields: {}",
                    field.name.value, valid_fields_text
                )
            };

            self.errors
                .push_back(SchemaError::Validation(message, field.span));
        }
    }

    pub(super) fn datasource_provider_value(datasource: &DatasourceDecl) -> Result<String> {
        let provider_field = datasource.find_field("provider").ok_or_else(|| {
            SchemaError::Validation(
                "Datasource missing required 'provider' field".to_string(),
                datasource.span,
            )
        })?;

        let provider = if let Expr::Literal(Literal::String(s, _)) = &provider_field.value {
            s.clone()
        } else {
            return Err(SchemaError::Validation(
                "Datasource 'provider' must be a string literal".to_string(),
                provider_field.span,
            ));
        };

        if provider.parse::<DatabaseProvider>().is_err() {
            return Err(SchemaError::Validation(
                format!(
                    "Unknown datasource provider '{}'. Valid providers: {}",
                    provider,
                    DatabaseProvider::ALL.join(", ")
                ),
                provider_field.span,
            ));
        }

        Ok(provider)
    }

    pub(super) fn datasource_url_value(datasource: &DatasourceDecl) -> Result<String> {
        Self::datasource_optional_url_value(datasource, "url")?.ok_or_else(|| {
            SchemaError::Validation(
                "Datasource missing required 'url' field".to_string(),
                datasource.span,
            )
        })
    }

    pub(super) fn datasource_direct_url_value(
        datasource: &DatasourceDecl,
    ) -> Result<Option<String>> {
        Self::datasource_optional_url_value(datasource, "direct_url")
    }

    pub(super) fn datasource_preserve_extensions_value(
        datasource: &DatasourceDecl,
    ) -> Result<bool> {
        let Some(field) = datasource.find_field("preserve_extensions") else {
            return Ok(false);
        };

        match &field.value {
            Expr::Literal(Literal::Boolean(value, _)) => Ok(*value),
            _ => Err(SchemaError::Validation(
                "Datasource 'preserve_extensions' must be a boolean literal".to_string(),
                field.span,
            )),
        }
    }

    fn datasource_optional_url_value(
        datasource: &DatasourceDecl,
        field_name: &str,
    ) -> Result<Option<String>> {
        let Some(url_field) = datasource.find_field(field_name) else {
            return Ok(None);
        };

        match &url_field.value {
            Expr::Literal(Literal::String(s, _)) => Ok(Some(s.clone())),
            Expr::FunctionCall { name, args, .. } if name.value == "env" => match args.as_slice() {
                [Expr::Literal(Literal::String(var_name, _))] => {
                    Ok(Some(format!("env({})", var_name)))
                }
                _ => Err(SchemaError::Validation(
                    format!(
                        "Datasource '{}' env() call requires a single string argument",
                        field_name
                    ),
                    url_field.span,
                )),
            },
            _ => Err(SchemaError::Validation(
                format!(
                    "Datasource '{}' must be a string literal or env() call",
                    field_name
                ),
                url_field.span,
            )),
        }
    }

    pub(super) fn generator_provider_info(
        generator: &GeneratorDecl,
    ) -> Result<(String, ClientProvider)> {
        let provider_field = generator.find_field("provider").ok_or_else(|| {
            SchemaError::Validation(
                "Generator missing required 'provider' field".to_string(),
                generator.span,
            )
        })?;

        let provider = if let Expr::Literal(Literal::String(s, _)) = &provider_field.value {
            s.clone()
        } else {
            return Err(SchemaError::Validation(
                "Generator 'provider' must be a string literal".to_string(),
                provider_field.span,
            ));
        };

        let client_provider = if let Ok(provider_kind) = provider.parse::<ClientProvider>() {
            provider_kind
        } else {
            return Err(SchemaError::Validation(
                format!(
                    "Unknown generator provider '{}'. Valid providers: {}",
                    provider,
                    ClientProvider::ALL.join(", ")
                ),
                provider_field.span,
            ));
        };

        Ok((provider, client_provider))
    }

    pub(super) fn generator_output_value(generator: &GeneratorDecl) -> Result<Option<String>> {
        let Some(output_field) = generator.find_field("output") else {
            return Ok(None);
        };

        match &output_field.value {
            Expr::Literal(Literal::String(s, _)) => Ok(Some(s.clone())),
            _ => Err(SchemaError::Validation(
                "Generator 'output' must be a string literal".to_string(),
                output_field.span,
            )),
        }
    }

    pub(super) fn generator_java_output_required(
        generator: &GeneratorDecl,
        client_provider: ClientProvider,
    ) -> Result<()> {
        if client_provider == ClientProvider::Java && generator.find_field("output").is_none() {
            return Err(SchemaError::Validation(
                "Generator field 'output' is required for 'nautilus-client-java'".to_string(),
                generator.span,
            ));
        }
        Ok(())
    }

    pub(super) fn generator_interface_kind(generator: &GeneratorDecl) -> Result<InterfaceKind> {
        let Some(iface_field) = generator.find_field("interface") else {
            return Ok(InterfaceKind::Sync);
        };

        let iface = if let Expr::Literal(Literal::String(s, _)) = &iface_field.value {
            s.as_str()
        } else {
            return Err(SchemaError::Validation(
                "Generator 'interface' must be a string literal".to_string(),
                iface_field.span,
            ));
        };

        match iface {
            "sync" => Ok(InterfaceKind::Sync),
            "async" | "asyncio" => Ok(InterfaceKind::Async),
            other => Err(SchemaError::Validation(
                format!(
                    "Invalid value '{}' for generator field 'interface'. \
                     Allowed values: \"sync\", \"async\"",
                    other
                ),
                iface_field.span,
            )),
        }
    }

    pub(super) fn generator_recursive_type_depth(
        generator: &GeneratorDecl,
        client_provider: ClientProvider,
        provider: &str,
    ) -> Result<usize> {
        let Some(depth_field) = generator.find_field("recursive_type_depth") else {
            return Ok(5);
        };

        if client_provider != ClientProvider::Python {
            return Err(SchemaError::Validation(
                format!(
                    "Field 'recursive_type_depth' is only supported for \
                     'nautilus-client-py', not for '{}'",
                    provider
                ),
                depth_field.span,
            ));
        }

        if let Expr::Literal(Literal::Number(s, _)) = &depth_field.value {
            Ok(s.parse::<usize>().unwrap_or(5).max(1))
        } else {
            Err(SchemaError::Validation(
                "Invalid value for generator field 'recursive_type_depth'. Expected a positive integer.".to_string(),
                depth_field.span,
            ))
        }
    }

    fn generator_java_string_field(
        generator: &GeneratorDecl,
        field_name: &str,
        client_provider: ClientProvider,
    ) -> Result<Option<String>> {
        let Some(field) = generator.find_field(field_name) else {
            return if client_provider == ClientProvider::Java {
                Err(SchemaError::Validation(
                    format!(
                        "Generator field '{}' is required for 'nautilus-client-java'",
                        field_name
                    ),
                    generator.span,
                ))
            } else {
                Ok(None)
            };
        };

        if client_provider != ClientProvider::Java {
            return Err(SchemaError::Validation(
                format!(
                    "Field '{}' is only supported for 'nautilus-client-java'",
                    field_name
                ),
                field.span,
            ));
        }

        match &field.value {
            Expr::Literal(Literal::String(value, _)) => Ok(Some(value.clone())),
            _ => Err(SchemaError::Validation(
                format!("Generator '{}' must be a string literal", field_name),
                field.span,
            )),
        }
    }

    pub(super) fn generator_java_package_value(
        generator: &GeneratorDecl,
        client_provider: ClientProvider,
    ) -> Result<Option<String>> {
        Self::generator_java_string_field(generator, "package", client_provider)
    }

    pub(super) fn generator_java_group_id_value(
        generator: &GeneratorDecl,
        client_provider: ClientProvider,
    ) -> Result<Option<String>> {
        Self::generator_java_string_field(generator, "group_id", client_provider)
    }

    pub(super) fn generator_java_artifact_id_value(
        generator: &GeneratorDecl,
        client_provider: ClientProvider,
    ) -> Result<Option<String>> {
        Self::generator_java_string_field(generator, "artifact_id", client_provider)
    }

    pub(super) fn generator_java_mode_value(
        generator: &GeneratorDecl,
        client_provider: ClientProvider,
    ) -> Result<Option<JavaGenerationMode>> {
        let Some(field) = generator.find_field("mode") else {
            return if client_provider == ClientProvider::Java {
                Ok(Some(JavaGenerationMode::Maven))
            } else {
                Ok(None)
            };
        };

        if client_provider != ClientProvider::Java {
            return Err(SchemaError::Validation(
                "Field 'mode' is only supported for 'nautilus-client-java'".to_string(),
                field.span,
            ));
        }

        let mode = match &field.value {
            Expr::Literal(Literal::String(value, _)) => value.as_str(),
            _ => {
                return Err(SchemaError::Validation(
                    "Generator 'mode' must be a string literal".to_string(),
                    field.span,
                ));
            }
        };

        match mode {
            "maven" => Ok(Some(JavaGenerationMode::Maven)),
            "jar" => Ok(Some(JavaGenerationMode::Jar)),
            other => Err(SchemaError::Validation(
                format!(
                    "Invalid value '{}' for generator field 'mode'. Allowed values: \"maven\", \"jar\"",
                    other
                ),
                field.span,
            )),
        }
    }

    pub(super) fn valid_generator_fields(
        client_provider: Option<ClientProvider>,
    ) -> Vec<&'static str> {
        let mut fields = KNOWN_GENERATOR_FIELDS.to_vec();
        if client_provider.is_none() || client_provider == Some(ClientProvider::Python) {
            fields.extend_from_slice(PYTHON_ONLY_GENERATOR_FIELDS);
        }
        if client_provider.is_none() || client_provider == Some(ClientProvider::Java) {
            fields.extend_from_slice(JAVA_ONLY_GENERATOR_FIELDS);
        }
        fields
    }
}

/// Result of parsing a single array entry in `extensions = [...]`.
#[derive(Debug, Clone)]
pub(super) struct ParsedExtensionEntry {
    pub(super) name: String,
    pub(super) schema: Option<String>,
    pub(super) span: Span,
}

fn extract_extension_string(expr: &Expr, span: Span) -> Result<String> {
    match expr {
        Expr::Ident(ident) => Ok(ident.value.clone()),
        Expr::Literal(Literal::String(s, _)) => Ok(s.clone()),
        _ => Err(SchemaError::Validation(
            "Extension argument must be an identifier or string literal".to_string(),
            span,
        )),
    }
}
