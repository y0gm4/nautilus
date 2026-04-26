//! Recursive descent parser for the nautilus schema language.
//!
//! This module provides a parser that transforms a stream of tokens into an AST.
//!
//! # Example
//!
//! ```ignore
//! use nautilus_schema::{Lexer, Parser};
//!
//! let source = r#"
//!     model User {
//!       id    Int    @id
//!       email String @unique
//!     }
//! "#;
//!
//! let tokens = Lexer::new(source).collect::<Result<Vec<_>, _>>().unwrap();
//! let schema = Parser::new(&tokens, source).parse_schema().unwrap();
//! ```

use crate::ast::*;
use crate::error::{Result, SchemaError};
use crate::span::Span;
use crate::token::{Token, TokenKind};

/// Parser for schema files.
pub struct Parser<'a> {
    /// Token stream.
    tokens: &'a [Token],
    /// Current position in token stream.
    pos: usize,
    /// Errors collected during error-recovery (non-fatal parse failures).
    recovered_errors: Vec<crate::error::SchemaError>,
}

impl<'a> Parser<'a> {
    /// Creates a new parser from a token slice and the original source text.
    pub fn new(tokens: &'a [Token], source: &'a str) -> Self {
        let _ = source; // kept in signature for API compatibility
        Self {
            tokens,
            pos: 0,
            recovered_errors: Vec::new(),
        }
    }

    /// Returns all errors that were silently recovered from during parsing.
    ///
    /// These are non-fatal: the parser managed to continue past them by
    /// skipping to the next top-level declaration.  Call this after
    /// [`parse_schema`] to collect the full set of parse diagnostics.
    pub fn take_errors(&mut self) -> Vec<crate::error::SchemaError> {
        std::mem::take(&mut self.recovered_errors)
    }

    /// Parses a complete schema.
    pub fn parse_schema(&mut self) -> Result<Schema> {
        let start = self.current_span();
        self.skip_newlines();

        let mut declarations = Vec::new();

        while !self.is_at_end() {
            match self.parse_declaration() {
                Ok(decl) => declarations.push(decl),
                Err(e) => {
                    // Error recovery: record the error and skip to the next declaration.
                    self.recovered_errors.push(e);
                    self.recover_to_next_declaration();
                }
            }
            self.skip_newlines();
        }

        let end = self.previous_span();
        Ok(Schema::new(declarations, start.merge(end)))
    }

    /// Parses a top-level declaration.
    fn parse_declaration(&mut self) -> Result<Declaration> {
        self.skip_newlines();

        match self.peek_kind() {
            Some(TokenKind::Datasource) => Ok(Declaration::Datasource(self.parse_datasource()?)),
            Some(TokenKind::Generator) => Ok(Declaration::Generator(self.parse_generator()?)),
            Some(TokenKind::Model) => Ok(Declaration::Model(self.parse_model()?)),
            Some(TokenKind::Enum) => Ok(Declaration::Enum(self.parse_enum()?)),
            Some(TokenKind::Type) => Ok(Declaration::Type(self.parse_type_decl()?)),
            Some(kind) => Err(SchemaError::Parse(
                format!("Expected declaration, found {:?}", kind),
                self.current_span(),
            )),
            None => Err(SchemaError::Parse(
                "Unexpected end of file".to_string(),
                self.current_span(),
            )),
        }
    }

    /// Parses a datasource block.
    fn parse_datasource(&mut self) -> Result<DatasourceDecl> {
        let start = self.expect(TokenKind::Datasource)?.span;
        let name = self.parse_ident()?;
        self.expect(TokenKind::LBrace)?;
        self.skip_newlines();

        let mut fields = Vec::new();
        while !self.check(TokenKind::RBrace) && !self.is_at_end() {
            fields.push(self.parse_config_field()?);
            self.skip_newlines();
        }

        let end = self.expect(TokenKind::RBrace)?.span;
        Ok(DatasourceDecl {
            name,
            fields,
            span: start.merge(end),
        })
    }

    /// Parses a generator block.
    fn parse_generator(&mut self) -> Result<GeneratorDecl> {
        let start = self.expect(TokenKind::Generator)?.span;
        let name = self.parse_ident()?;
        self.expect(TokenKind::LBrace)?;
        self.skip_newlines();

        let mut fields = Vec::new();
        while !self.check(TokenKind::RBrace) && !self.is_at_end() {
            fields.push(self.parse_config_field()?);
            self.skip_newlines();
        }

        let end = self.expect(TokenKind::RBrace)?.span;
        Ok(GeneratorDecl {
            name,
            fields,
            span: start.merge(end),
        })
    }

    /// Parses a configuration field (key = value).
    fn parse_config_field(&mut self) -> Result<ConfigField> {
        let name = self.parse_ident()?;
        self.expect(TokenKind::Equal)?;
        let value = self.parse_expr()?;
        let span = name.span.merge(value.span());
        Ok(ConfigField { name, value, span })
    }

    /// Parses a model block.
    fn parse_model(&mut self) -> Result<ModelDecl> {
        let start = self.expect(TokenKind::Model)?.span;
        let name = self.parse_ident()?;
        self.expect(TokenKind::LBrace)?;
        self.skip_newlines();

        let mut fields = Vec::new();
        let mut attributes = Vec::new();

        while !self.check(TokenKind::RBrace) && !self.is_at_end() {
            if self.check(TokenKind::AtAt) {
                attributes.push(self.parse_model_attribute()?);
            } else {
                fields.push(self.parse_field_decl()?);
            }
            self.skip_newlines();
        }

        let end = self.expect(TokenKind::RBrace)?.span;
        Ok(ModelDecl {
            name,
            fields,
            attributes,
            span: start.merge(end),
        })
    }

    /// Parses a composite type block.
    fn parse_type_decl(&mut self) -> Result<TypeDecl> {
        let start = self.expect(TokenKind::Type)?.span;
        let name = self.parse_ident()?;
        self.expect(TokenKind::LBrace)?;
        self.skip_newlines();

        let mut fields = Vec::new();

        while !self.check(TokenKind::RBrace) && !self.is_at_end() {
            // Type blocks do not support @@ model-level attributes
            fields.push(self.parse_field_decl()?);
            self.skip_newlines();
        }

        let end = self.expect(TokenKind::RBrace)?.span;
        Ok(TypeDecl {
            name,
            fields,
            span: start.merge(end),
        })
    }

    /// Parses a field declaration.
    fn parse_field_decl(&mut self) -> Result<FieldDecl> {
        let name = self.parse_ident()?;
        let field_type = self.parse_field_type()?;
        let modifier = self.parse_field_modifier()?;
        let base_span = name.span.merge(self.previous_span());

        let mut attributes = Vec::new();
        while self.check(TokenKind::At) && !self.check(TokenKind::AtAt) {
            attributes.push(self.parse_field_attribute()?);
        }

        let span = if let Some(last_attr) = attributes.last() {
            match last_attr {
                FieldAttribute::Id => base_span,
                FieldAttribute::Unique => base_span,
                FieldAttribute::UpdatedAt { span } => base_span.merge(*span),
                FieldAttribute::Default(_, span) => base_span.merge(*span),
                FieldAttribute::Map(_) => base_span,
                FieldAttribute::Store { .. } => base_span,
                FieldAttribute::Relation { span, .. } => base_span.merge(*span),
                FieldAttribute::Computed { span, .. } => base_span.merge(*span),
                FieldAttribute::Check { span, .. } => base_span.merge(*span),
            }
        } else {
            base_span
        };

        Ok(FieldDecl {
            name,
            field_type,
            modifier,
            attributes,
            span,
        })
    }

    /// Parses a field type.
    fn parse_field_type(&mut self) -> Result<FieldType> {
        let ident = self.parse_ident()?;

        let field_type = match ident.value.as_str() {
            "String" => FieldType::String,
            "Boolean" => FieldType::Boolean,
            "Int" => FieldType::Int,
            "BigInt" => FieldType::BigInt,
            "Float" => FieldType::Float,
            "DateTime" => FieldType::DateTime,
            "Bytes" => FieldType::Bytes,
            "Json" => FieldType::Json,
            "Uuid" => FieldType::Uuid,
            "Citext" => FieldType::Citext,
            "Hstore" => FieldType::Hstore,
            "Ltree" => FieldType::Ltree,
            "Vector" => {
                self.expect(TokenKind::LParen)?;
                let dimension = self.parse_number()?.parse::<u32>().map_err(|_| {
                    SchemaError::Parse(
                        "Invalid vector dimension value".to_string(),
                        self.current_span(),
                    )
                })?;
                self.expect(TokenKind::RParen)?;
                FieldType::Vector { dimension }
            }
            "Jsonb" => FieldType::Jsonb,
            "Xml" => FieldType::Xml,
            "Char" => {
                self.expect(TokenKind::LParen)?;
                let length = self.parse_number()?.parse::<u32>().map_err(|_| {
                    SchemaError::Parse("Invalid length value".to_string(), self.current_span())
                })?;
                self.expect(TokenKind::RParen)?;
                FieldType::Char { length }
            }
            "VarChar" => {
                self.expect(TokenKind::LParen)?;
                let length = self.parse_number()?.parse::<u32>().map_err(|_| {
                    SchemaError::Parse("Invalid length value".to_string(), self.current_span())
                })?;
                self.expect(TokenKind::RParen)?;
                FieldType::VarChar { length }
            }
            "Decimal" => {
                if self.check(TokenKind::LParen) {
                    self.advance();
                    let precision = self.parse_number()?.parse::<u32>().map_err(|_| {
                        SchemaError::Parse(
                            "Invalid precision value".to_string(),
                            self.current_span(),
                        )
                    })?;
                    self.expect(TokenKind::Comma)?;
                    let scale = self.parse_number()?.parse::<u32>().map_err(|_| {
                        SchemaError::Parse("Invalid scale value".to_string(), self.current_span())
                    })?;
                    self.expect(TokenKind::RParen)?;
                    FieldType::Decimal { precision, scale }
                } else {
                    return Err(SchemaError::Parse(
                        "Decimal type requires precision and scale: Decimal(p, s)".to_string(),
                        ident.span,
                    ));
                }
            }
            _ => FieldType::UserType(ident.value),
        };

        Ok(field_type)
    }

    /// Parses field modifiers (?, !, or []).
    fn parse_field_modifier(&mut self) -> Result<FieldModifier> {
        if self.check(TokenKind::Question) {
            self.advance();
            Ok(FieldModifier::Optional)
        } else if self.check(TokenKind::Bang) {
            self.advance();
            Ok(FieldModifier::NotNull)
        } else if self.check(TokenKind::LBracket) {
            self.advance();
            self.expect(TokenKind::RBracket)?;
            Ok(FieldModifier::Array)
        } else {
            Ok(FieldModifier::None)
        }
    }

    /// Parses a field attribute (@id, @unique, etc.).
    fn parse_field_attribute(&mut self) -> Result<FieldAttribute> {
        let at_token = self.expect(TokenKind::At)?;
        let at_span = at_token.span;
        let name = self.parse_ident()?;

        match name.value.as_str() {
            "id" => Ok(FieldAttribute::Id),
            "unique" => Ok(FieldAttribute::Unique),
            "updatedAt" => Ok(FieldAttribute::UpdatedAt {
                span: at_span.merge(name.span),
            }),
            "default" => {
                self.expect(TokenKind::LParen)?;
                let expr = self.parse_expr()?;
                let rparen = self.expect(TokenKind::RParen)?;
                let full_span = at_span.merge(rparen.span);
                Ok(FieldAttribute::Default(expr, full_span))
            }
            "map" => {
                self.expect(TokenKind::LParen)?;
                let map_name = self.parse_string()?;
                self.expect(TokenKind::RParen)?;
                Ok(FieldAttribute::Map(map_name))
            }
            "store" => {
                let start = name.span;
                self.expect(TokenKind::LParen)?;
                let strategy_ident = self.parse_ident()?;
                let strategy = match strategy_ident.value.as_str() {
                    "json" => StorageStrategy::Json,
                    "native" => StorageStrategy::Native,
                    _ => {
                        return Err(SchemaError::Parse(
                            format!(
                                "Unknown storage strategy: '{}'. Valid options: 'json', 'native'",
                                strategy_ident.value
                            ),
                            strategy_ident.span,
                        ))
                    }
                };
                let end = self.expect(TokenKind::RParen)?.span;
                Ok(FieldAttribute::Store {
                    strategy,
                    span: start.merge(end),
                })
            }
            "relation" => {
                let start = name.span;
                self.expect(TokenKind::LParen)?;

                let mut rel_name = None;
                let mut fields = None;
                let mut references = None;
                let mut on_delete = None;
                let mut on_update = None;

                while !self.check(TokenKind::RParen) && !self.is_at_end() {
                    let arg_name = self.parse_ident()?;
                    self.expect(TokenKind::Colon)?;

                    match arg_name.value.as_str() {
                        "name" => rel_name = Some(self.parse_string()?),
                        "fields" => fields = Some(self.parse_ident_array()?),
                        "references" => references = Some(self.parse_ident_array()?),
                        "onDelete" => on_delete = Some(self.parse_referential_action()?),
                        "onUpdate" => on_update = Some(self.parse_referential_action()?),
                        _ => {
                            return Err(SchemaError::Parse(
                                format!("Unknown relation argument: {}", arg_name.value),
                                arg_name.span,
                            ))
                        }
                    }

                    if self.check(TokenKind::Comma) {
                        self.advance();
                    }
                }

                let end = self.expect(TokenKind::RParen)?.span;
                Ok(FieldAttribute::Relation {
                    name: rel_name,
                    fields,
                    references,
                    on_delete,
                    on_update,
                    span: start.merge(end),
                })
            }
            "computed" => {
                let start = at_span;
                self.expect(TokenKind::LParen)?;
                let expr = self.parse_sql_expr()?;
                self.expect(TokenKind::Comma)?;
                let kind_ident = self.parse_ident()?;
                let kind = match kind_ident.value.as_str() {
                    "Stored" => ComputedKind::Stored,
                    "Virtual" => ComputedKind::Virtual,
                    other => {
                        return Err(SchemaError::Parse(
                            format!(
                                "Unknown computed kind '{}'. Valid options: Stored, Virtual",
                                other
                            ),
                            kind_ident.span,
                        ))
                    }
                };
                let end = self.expect(TokenKind::RParen)?.span;
                Ok(FieldAttribute::Computed {
                    expr,
                    kind,
                    span: start.merge(end),
                })
            }
            "check" => {
                let start = at_span;
                self.expect(TokenKind::LParen)?;
                let expr = self.parse_bool_expr()?;
                let end = self.expect(TokenKind::RParen)?.span;
                Ok(FieldAttribute::Check {
                    expr,
                    span: start.merge(end),
                })
            }
            _ => Err(SchemaError::Parse(
                format!("Unknown field attribute: @{}", name.value),
                name.span,
            )),
        }
    }

    /// Collects SQL expression tokens until a top-level comma or closing paren,
    /// then parses them into a validated [`SqlExpr`] tree.
    fn parse_sql_expr(&mut self) -> Result<crate::sql_expr::SqlExpr> {
        if self.pos >= self.tokens.len() {
            return Err(SchemaError::Parse(
                "Unexpected end of file in @computed expression".to_string(),
                self.current_span(),
            ));
        }
        let fallback_span = self.current_span();
        let expr_start = self.pos;
        let mut depth: i32 = 0;

        loop {
            match self.peek_kind() {
                Some(TokenKind::LParen) => {
                    depth += 1;
                    self.advance();
                }
                Some(TokenKind::RParen) if depth == 0 => break,
                Some(TokenKind::RParen) => {
                    depth -= 1;
                    self.advance();
                }
                Some(TokenKind::Comma) if depth == 0 => break,
                None | Some(TokenKind::Eof) => {
                    return Err(SchemaError::Parse(
                        "Unexpected end of file in @computed expression".to_string(),
                        self.current_span(),
                    ));
                }
                _ => {
                    self.advance();
                }
            }
        }

        let expr_tokens: Vec<_> = self.tokens[expr_start..self.pos]
            .iter()
            .filter(|t| !matches!(t.kind, TokenKind::Newline))
            .cloned()
            .collect();

        crate::sql_expr::parse_sql_expr(&expr_tokens, fallback_span)
    }

    /// Collects boolean expression tokens until a top-level closing paren,
    /// then parses them into a validated [`BoolExpr`] tree.
    fn parse_bool_expr(&mut self) -> Result<crate::bool_expr::BoolExpr> {
        if self.pos >= self.tokens.len() {
            return Err(SchemaError::Parse(
                "Unexpected end of file in @check expression".to_string(),
                self.current_span(),
            ));
        }
        let fallback_span = self.current_span();
        let expr_start = self.pos;
        let mut depth: i32 = 0;

        loop {
            match self.peek_kind() {
                Some(TokenKind::LParen) => {
                    depth += 1;
                    self.advance();
                }
                Some(TokenKind::RParen) if depth == 0 => break,
                Some(TokenKind::RParen) => {
                    depth -= 1;
                    self.advance();
                }
                None | Some(TokenKind::Eof) => {
                    return Err(SchemaError::Parse(
                        "Unexpected end of file in @check expression".to_string(),
                        self.current_span(),
                    ));
                }
                _ => {
                    self.advance();
                }
            }
        }

        let expr_tokens: Vec<_> = self.tokens[expr_start..self.pos]
            .iter()
            .filter(|t| !matches!(t.kind, TokenKind::Newline))
            .cloned()
            .collect();

        crate::bool_expr::parse_bool_expr(&expr_tokens, fallback_span)
    }

    /// Parses a model attribute (@@map, @@id, etc.).
    fn parse_model_attribute(&mut self) -> Result<ModelAttribute> {
        self.expect(TokenKind::AtAt)?;
        let name = self.parse_ident()?;

        match name.value.as_str() {
            "map" => {
                self.expect(TokenKind::LParen)?;
                let map_name = self.parse_string()?;
                self.expect(TokenKind::RParen)?;
                Ok(ModelAttribute::Map(map_name))
            }
            "id" => {
                self.expect(TokenKind::LParen)?;
                let fields = self.parse_ident_array()?;
                self.expect(TokenKind::RParen)?;
                Ok(ModelAttribute::Id(fields))
            }
            "unique" => {
                self.expect(TokenKind::LParen)?;
                let fields = self.parse_ident_array()?;
                self.expect(TokenKind::RParen)?;
                Ok(ModelAttribute::Unique(fields))
            }
            "index" => {
                self.expect(TokenKind::LParen)?;
                let fields = self.parse_ident_array()?;
                let mut index_type: Option<Ident> = None;
                let mut opclass: Option<Ident> = None;
                let mut m: Option<u32> = None;
                let mut ef_construction: Option<u32> = None;
                let mut lists: Option<u32> = None;
                let mut index_name: Option<String> = None;
                let mut index_map: Option<String> = None;
                while self.check(TokenKind::Comma) {
                    self.advance();
                    if self.check(TokenKind::RParen) {
                        break;
                    }
                    let key = self.parse_ident()?;
                    self.expect(TokenKind::Colon)?;
                    match key.value.as_str() {
                        "type" => {
                            index_type = Some(self.parse_ident()?);
                        }
                        "opclass" => {
                            opclass = Some(self.parse_ident()?);
                        }
                        "m" => {
                            m = Some(self.parse_u32_literal("m")?);
                        }
                        "ef_construction" => {
                            ef_construction = Some(self.parse_u32_literal("ef_construction")?);
                        }
                        "lists" => {
                            lists = Some(self.parse_u32_literal("lists")?);
                        }
                        "name" => {
                            index_name = Some(self.parse_string()?);
                        }
                        "map" => {
                            index_map = Some(self.parse_string()?);
                        }
                        _ => {
                            return Err(SchemaError::Parse(
                                format!("Unknown @@index argument: '{}'", key.value),
                                key.span,
                            ));
                        }
                    }
                }
                self.expect(TokenKind::RParen)?;
                Ok(ModelAttribute::Index {
                    fields,
                    index_type,
                    opclass,
                    m,
                    ef_construction,
                    lists,
                    name: index_name,
                    map: index_map,
                })
            }
            "check" => {
                self.expect(TokenKind::LParen)?;
                let expr = self.parse_bool_expr()?;
                let end = self.expect(TokenKind::RParen)?.span;
                Ok(ModelAttribute::Check {
                    expr,
                    span: name.span.merge(end),
                })
            }
            _ => Err(SchemaError::Parse(
                format!("Unknown model attribute: @@{}", name.value),
                name.span,
            )),
        }
    }

    /// Parses an array of identifiers [a, b, c].
    fn parse_ident_array(&mut self) -> Result<Vec<Ident>> {
        self.expect(TokenKind::LBracket)?;
        let mut idents = Vec::new();

        while !self.check(TokenKind::RBracket) && !self.is_at_end() {
            idents.push(self.parse_ident()?);
            if self.check(TokenKind::Comma) {
                self.advance();
            }
        }

        self.expect(TokenKind::RBracket)?;
        Ok(idents)
    }

    /// Parses a referential action (Cascade, SetNull, etc.).
    fn parse_referential_action(&mut self) -> Result<ReferentialAction> {
        let ident = self.parse_ident()?;
        match ident.value.as_str() {
            "Cascade" => Ok(ReferentialAction::Cascade),
            "Restrict" => Ok(ReferentialAction::Restrict),
            "NoAction" => Ok(ReferentialAction::NoAction),
            "SetNull" => Ok(ReferentialAction::SetNull),
            "SetDefault" => Ok(ReferentialAction::SetDefault),
            _ => Err(SchemaError::Parse(
                format!("Unknown referential action: {}", ident.value),
                ident.span,
            )),
        }
    }

    /// Parses an enum block.
    fn parse_enum(&mut self) -> Result<EnumDecl> {
        let start = self.expect(TokenKind::Enum)?.span;
        let name = self.parse_ident()?;
        self.expect(TokenKind::LBrace)?;
        self.skip_newlines();

        let mut variants = Vec::new();
        while !self.check(TokenKind::RBrace) && !self.is_at_end() {
            let variant_name = self.parse_ident()?;
            let variant_span = variant_name.span;
            variants.push(EnumVariant {
                name: variant_name,
                span: variant_span,
            });
            self.skip_newlines();
        }

        let end = self.expect(TokenKind::RBrace)?.span;
        Ok(EnumDecl {
            name,
            variants,
            span: start.merge(end),
        })
    }

    /// Parses an expression.
    fn parse_expr(&mut self) -> Result<Expr> {
        match self.peek_kind() {
            Some(TokenKind::String(_)) => {
                let s = self.parse_string()?;
                let span = self.previous_span();
                Ok(Expr::Literal(Literal::String(s, span)))
            }
            Some(TokenKind::Number(_)) => {
                let n = self.parse_number()?;
                let span = self.previous_span();
                Ok(Expr::Literal(Literal::Number(n, span)))
            }
            Some(TokenKind::True) => {
                let span = self.advance().span;
                Ok(Expr::Literal(Literal::Boolean(true, span)))
            }
            Some(TokenKind::False) => {
                let span = self.advance().span;
                Ok(Expr::Literal(Literal::Boolean(false, span)))
            }
            Some(TokenKind::LBracket) => self.parse_array_expr(),
            Some(TokenKind::Ident(_)) => {
                let ident = self.parse_ident()?;

                if self.check(TokenKind::LParen) {
                    let start = ident.span;
                    self.advance();

                    let mut args = Vec::new();
                    while !self.check(TokenKind::RParen) && !self.is_at_end() {
                        args.push(self.parse_call_argument()?);
                        if self.check(TokenKind::Comma) {
                            self.advance();
                        }
                    }

                    let end = self.expect(TokenKind::RParen)?.span;
                    Ok(Expr::FunctionCall {
                        name: ident,
                        args,
                        span: start.merge(end),
                    })
                } else {
                    Ok(Expr::Ident(ident))
                }
            }
            Some(kind) => Err(SchemaError::Parse(
                format!("Expected expression, found {:?}", kind),
                self.current_span(),
            )),
            None => Err(SchemaError::Parse(
                "Unexpected end of file in expression".to_string(),
                self.current_span(),
            )),
        }
    }

    /// Parses a single function-call argument.
    ///
    /// Supports two forms:
    /// - positional: any `parse_expr` value;
    /// - named: `ident = expr`, emitted as [`Expr::NamedArg`]. This is what
    ///   the structured `extension(name = ..., schema = ...)` datasource
    ///   entry relies on.
    fn parse_call_argument(&mut self) -> Result<Expr> {
        if let Some(TokenKind::Ident(_)) = self.peek_kind() {
            if matches!(self.peek_kind_at(1), Some(TokenKind::Equal)) {
                let name = self.parse_ident()?;
                self.expect(TokenKind::Equal)?;
                let value = self.parse_expr()?;
                let span = name.span.merge(value.span());
                return Ok(Expr::NamedArg {
                    name,
                    value: Box::new(value),
                    span,
                });
            }
        }
        self.parse_expr()
    }

    /// Parses an array expression [a, b, c].
    fn parse_array_expr(&mut self) -> Result<Expr> {
        let start = self.expect(TokenKind::LBracket)?.span;
        let mut elements = Vec::new();

        while !self.check(TokenKind::RBracket) && !self.is_at_end() {
            elements.push(self.parse_expr()?);
            if self.check(TokenKind::Comma) {
                self.advance();
            }
        }

        let end = self.expect(TokenKind::RBracket)?.span;
        Ok(Expr::Array {
            elements,
            span: start.merge(end),
        })
    }

    /// Parses an identifier.
    fn parse_ident(&mut self) -> Result<Ident> {
        match self.peek_kind() {
            Some(TokenKind::Ident(ref name)) => {
                let name = name.clone();
                let span = self.advance().span;
                Ok(Ident::new(name, span))
            }
            // Allow `type` keyword as an identifier in contexts like
            // named arguments (e.g., `type: Hash` in @@index)
            Some(TokenKind::Type) => {
                let span = self.advance().span;
                Ok(Ident::new("type".to_string(), span))
            }
            Some(kind) => Err(SchemaError::Parse(
                format!("Expected identifier, found {:?}", kind),
                self.current_span(),
            )),
            None => Err(SchemaError::Parse(
                "Expected identifier, found EOF".to_string(),
                self.current_span(),
            )),
        }
    }

    /// Parses a string literal.
    fn parse_string(&mut self) -> Result<String> {
        match self.peek_kind() {
            Some(TokenKind::String(ref s)) => {
                let s = s.clone();
                self.advance();
                Ok(s)
            }
            Some(kind) => Err(SchemaError::Parse(
                format!("Expected string, found {:?}", kind),
                self.current_span(),
            )),
            None => Err(SchemaError::Parse(
                "Expected string, found EOF".to_string(),
                self.current_span(),
            )),
        }
    }

    /// Parses a number literal.
    fn parse_number(&mut self) -> Result<String> {
        match self.peek_kind() {
            Some(TokenKind::Number(ref n)) => {
                let n = n.clone();
                self.advance();
                Ok(n)
            }
            Some(kind) => Err(SchemaError::Parse(
                format!("Expected number, found {:?}", kind),
                self.current_span(),
            )),
            None => Err(SchemaError::Parse(
                "Expected number, found EOF".to_string(),
                self.current_span(),
            )),
        }
    }

    /// Parses an unsigned integer literal used by index arguments.
    fn parse_u32_literal(&mut self, argument_name: &str) -> Result<u32> {
        let raw = self.parse_number()?;
        let span = self.previous_span();
        raw.parse::<u32>().map_err(|_| {
            SchemaError::Parse(
                format!(
                    "Expected '{}' to be a non-negative integer literal, found '{}'",
                    argument_name, raw
                ),
                span,
            )
        })
    }

    /// Checks if current token matches the given kind.
    fn check(&self, kind: TokenKind) -> bool {
        self.peek_kind()
            .map(|k| std::mem::discriminant(&k) == std::mem::discriminant(&kind))
            .unwrap_or(false)
    }

    /// Expects the current token to be of the given kind, advances, returns token.
    fn expect(&mut self, kind: TokenKind) -> Result<&'a Token> {
        if self.check(kind.clone()) {
            Ok(self.advance())
        } else {
            Err(SchemaError::Parse(
                format!("Expected {:?}, found {:?}", kind, self.peek_kind()),
                self.current_span(),
            ))
        }
    }

    /// Peeks at the current token kind.
    fn peek_kind(&self) -> Option<TokenKind> {
        self.tokens.get(self.pos).map(|t| t.kind.clone())
    }

    /// Peeks at the token kind `offset` positions ahead of the cursor.
    ///
    /// Used for single-token lookahead (e.g. deciding whether an identifier
    /// followed by `=` should be parsed as a named function-call argument).
    fn peek_kind_at(&self, offset: usize) -> Option<TokenKind> {
        self.tokens.get(self.pos + offset).map(|t| t.kind.clone())
    }

    /// Advances to the next token, returns current token.
    fn advance(&mut self) -> &'a Token {
        let token = &self.tokens[self.pos];
        if self.pos < self.tokens.len() {
            self.pos += 1;
        }
        token
    }

    /// Returns the span of the current token.
    fn current_span(&self) -> Span {
        self.tokens
            .get(self.pos)
            .map(|t| t.span)
            .unwrap_or_else(|| self.previous_span())
    }

    /// Returns the span of the previous token.
    fn previous_span(&self) -> Span {
        if self.pos > 0 {
            self.tokens[self.pos - 1].span
        } else {
            Span::new(0, 0)
        }
    }

    /// Checks if we're at the end of the token stream.
    fn is_at_end(&self) -> bool {
        self.pos >= self.tokens.len() || matches!(self.peek_kind(), Some(TokenKind::Eof))
    }

    /// Skips newline tokens.
    fn skip_newlines(&mut self) {
        while matches!(self.peek_kind(), Some(TokenKind::Newline)) {
            self.advance();
        }
    }

    /// Recovers to the next declaration (for error recovery).
    fn recover_to_next_declaration(&mut self) {
        while !self.is_at_end() {
            match self.peek_kind() {
                Some(TokenKind::Datasource)
                | Some(TokenKind::Generator)
                | Some(TokenKind::Model)
                | Some(TokenKind::Enum)
                | Some(TokenKind::Type) => break,
                _ => {
                    self.advance();
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lexer::Lexer;

    fn tokenize(input: &str) -> Vec<Token> {
        let mut lexer = Lexer::new(input);
        let mut tokens = Vec::new();
        loop {
            match lexer.next_token() {
                Ok(token) => {
                    if matches!(token.kind, TokenKind::Eof) {
                        tokens.push(token);
                        break;
                    }
                    tokens.push(token);
                }
                Err(e) => panic!("Tokenization failed: {}", e),
            }
        }
        tokens
    }

    #[test]
    fn test_parse_empty_schema() {
        let input = "";
        let tokens = tokenize(input);
        let schema = Parser::new(&tokens, input).parse_schema().unwrap();
        assert_eq!(schema.declarations.len(), 0);
    }

    #[test]
    fn test_parse_simple_model() {
        let input = r#"
            model User {
                id Int @id
            }
        "#;
        let tokens = tokenize(input);
        let schema = Parser::new(&tokens, input).parse_schema().unwrap();
        assert_eq!(schema.declarations.len(), 1);

        match &schema.declarations[0] {
            Declaration::Model(model) => {
                assert_eq!(model.name.value, "User");
                assert_eq!(model.fields.len(), 1);
                assert_eq!(model.fields[0].name.value, "id");
            }
            _ => panic!("Expected model declaration"),
        }
    }

    #[test]
    fn test_parse_field_types() {
        let input = r#"
            model Test {
                str String
                num Int
                big BigInt
                opt String?
                arr Int[]
                dec Decimal(10, 2)
            }
        "#;
        let tokens = tokenize(input);
        let schema = Parser::new(&tokens, input).parse_schema().unwrap();

        match &schema.declarations[0] {
            Declaration::Model(model) => {
                assert_eq!(model.fields.len(), 6);
                assert!(matches!(model.fields[0].field_type, FieldType::String));
                assert!(matches!(model.fields[1].field_type, FieldType::Int));
                assert!(matches!(model.fields[2].field_type, FieldType::BigInt));
                assert!(model.fields[3].is_optional());
                assert!(model.fields[4].is_array());
                assert!(matches!(
                    model.fields[5].field_type,
                    FieldType::Decimal {
                        precision: 10,
                        scale: 2
                    }
                ));
            }
            _ => panic!("Expected model"),
        }
    }

    #[test]
    fn test_parse_field_attributes() {
        let input = r#"
            model User {
                id Int @id @default(autoincrement())
                email String @unique @map("user_email")
            }
        "#;
        let tokens = tokenize(input);
        let schema = Parser::new(&tokens, input).parse_schema().unwrap();

        match &schema.declarations[0] {
            Declaration::Model(model) => {
                assert_eq!(model.fields[0].attributes.len(), 2);
                assert!(matches!(model.fields[0].attributes[0], FieldAttribute::Id));
                assert!(matches!(
                    model.fields[0].attributes[1],
                    FieldAttribute::Default(..)
                ));

                assert_eq!(model.fields[1].attributes.len(), 2);
                assert!(matches!(
                    model.fields[1].attributes[0],
                    FieldAttribute::Unique
                ));
                assert!(matches!(
                    model.fields[1].attributes[1],
                    FieldAttribute::Map(_)
                ));
            }
            _ => panic!("Expected model"),
        }
    }

    #[test]
    fn test_parse_model_attributes() {
        let input = r#"
            model User {
                id Int
                @@map("users")
                @@id([id])
            }
        "#;
        let tokens = tokenize(input);
        let schema = Parser::new(&tokens, input).parse_schema().unwrap();

        match &schema.declarations[0] {
            Declaration::Model(model) => {
                assert_eq!(model.attributes.len(), 2);
                assert!(matches!(model.attributes[0], ModelAttribute::Map(_)));
                assert!(matches!(model.attributes[1], ModelAttribute::Id(_)));
            }
            _ => panic!("Expected model"),
        }
    }

    #[test]
    fn test_parse_enum() {
        let input = r#"
            enum Role {
                USER
                ADMIN
            }
        "#;
        let tokens = tokenize(input);
        let schema = Parser::new(&tokens, input).parse_schema().unwrap();

        match &schema.declarations[0] {
            Declaration::Enum(enum_decl) => {
                assert_eq!(enum_decl.name.value, "Role");
                assert_eq!(enum_decl.variants.len(), 2);
                assert_eq!(enum_decl.variants[0].name.value, "USER");
                assert_eq!(enum_decl.variants[1].name.value, "ADMIN");
            }
            _ => panic!("Expected enum"),
        }
    }

    #[test]
    fn test_parse_datasource() {
        let input = r#"
            datasource db {
                provider = "postgresql"
                url = env("DATABASE_URL")
            }
        "#;
        let tokens = tokenize(input);
        let schema = Parser::new(&tokens, input).parse_schema().unwrap();

        match &schema.declarations[0] {
            Declaration::Datasource(ds) => {
                assert_eq!(ds.name.value, "db");
                assert_eq!(ds.fields.len(), 2);
                assert_eq!(ds.provider(), Some("postgresql"));
            }
            _ => panic!("Expected datasource"),
        }
    }

    #[test]
    fn test_parse_generator() {
        let input = r#"
            generator client {
                provider = "nautilus-client-rs"
                output = "../generated"
            }
        "#;
        let tokens = tokenize(input);
        let schema = Parser::new(&tokens, input).parse_schema().unwrap();

        match &schema.declarations[0] {
            Declaration::Generator(gen) => {
                assert_eq!(gen.name.value, "client");
                assert_eq!(gen.fields.len(), 2);
            }
            _ => panic!("Expected generator"),
        }
    }

    #[test]
    fn test_parse_relation() {
        let input = r#"
            model Post {
                userId Int
                user User @relation(fields: [userId], references: [id], onDelete: Cascade)
            }
        "#;
        let tokens = tokenize(input);
        let schema = Parser::new(&tokens, input).parse_schema().unwrap();

        match &schema.declarations[0] {
            Declaration::Model(model) => match &model.fields[1].attributes[0] {
                FieldAttribute::Relation {
                    fields,
                    references,
                    on_delete,
                    ..
                } => {
                    assert!(fields.is_some());
                    assert!(references.is_some());
                    assert_eq!(*on_delete, Some(ReferentialAction::Cascade));
                }
                _ => panic!("Expected relation attribute"),
            },
            _ => panic!("Expected model"),
        }
    }
}
