use convert_case::{Case, Casing};
use itertools::Itertools;
use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use syn::parse::Parser;
use syn::{
    parse_macro_input, punctuated::Punctuated, Data, DeriveInput, Field, Fields, Lit, Meta,
    MetaNameValue, Token, Type,
};
use syn::{Attribute, Error, Expr, Ident, Result};

/// Derive macro for implementing the TryUpdateKey trait
///
/// This macro automatically implements TryUpdateKey for a struct,
/// mapping field names to configuration keys and using appropriate parsers
/// based on the field type.
///
/// Additional key aliases can be specified with the `#[delta(alias = "alias.name")]` attribute.
/// Multiple aliases can be added by `#[delta(alias = "foo", alias = "bar")]`.
///
/// Reading configuration can be achieved by assigning environmene keys to a field
/// `#[delta(env = "MY_ENV_KEY")]`.
#[proc_macro_derive(DeltaConfig, attributes(delta))]
pub fn derive_delta_config(input: TokenStream) -> TokenStream {
    // Parse the input tokens into a syntax tree
    let input = parse_macro_input!(input as DeriveInput);

    // Get the name of the struct
    let name = &input.ident;

    // Extract the fields from the struct
    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => &fields.named,
            _ => panic!("TryUpdateKey can only be derived for structs with named fields"),
        },
        _ => panic!("TryUpdateKey can only be derived for structs"),
    }
    .into_iter()
    .collect::<Vec<_>>();

    // Generate the implementation for TryUpdateKey trait
    let try_update_key = match generate_try_update_key(name, &fields) {
        Ok(try_update_key) => try_update_key,
        Err(err) => return syn::Error::into_compile_error(err).into(),
    };

    // generate an enum with all configuration keys
    let config_keys = match generate_config_keys(name, &fields) {
        Ok(config_keys) => config_keys,
        Err(err) => return syn::Error::into_compile_error(err).into(),
    };

    // generate a FromIterator implementation
    let from_iter = generate_from_iterator(name);

    let expanded = quote! {
        #try_update_key

        #config_keys

        #from_iter
    };

    TokenStream::from(expanded)
}

fn generate_config_keys(name: &Ident, fields: &[&Field]) -> Result<proc_macro2::TokenStream> {
    let enum_name = Ident::new(&format!("{name}Key"), Span::call_site());
    let variants: Vec<_> = fields
        .iter()
        .map(|field| {
            let field_name = &field
                .ident
                .as_ref()
                .ok_or_else(|| syn::Error::new_spanned(field, "expected name"))?
                .to_string();
            let pascal_case = Ident::new(&field_name.to_case(Case::Pascal), Span::call_site());
            let attributes = extract_field_attributes(&field.attrs)?;

            // Generate doc attribute if documentation exists
            let doc_attr = if let Some(doc_string) = attributes.docs {
                // Create a doc attribute for the enum variant
                quote! { #[doc = #doc_string] }
            } else {
                // No documentation
                quote! {}
            };

            // Return the variant with its documentation
            Ok(quote! {
                #doc_attr
                #pascal_case
            })
        })
        .collect::<Result<_>>()?;
    Ok(quote! {
        #[automatically_derived]
        pub enum #enum_name {
            #(#variants),*
        }
    })
}

fn generate_from_iterator(name: &Ident) -> proc_macro2::TokenStream {
    quote! {
        #[automatically_derived]
        impl<K, V> FromIterator<(K, V)> for #name
        where
            K: AsRef<str> + Into<String>,
            V: AsRef<str> + Into<String>,
        {
            fn from_iter<I: IntoIterator<Item = (K, V)>>(iter: I) -> Self {
                crate::logstore::config::ParseResult::from_iter(iter).config
            }
        }
    }
}

fn generate_try_update_key(name: &Ident, fields: &[&Field]) -> Result<proc_macro2::TokenStream> {
    let match_arms: Vec<_> = fields
        .iter()
        .filter_map(|field| {
            let field_name = &field.ident.as_ref().unwrap();
            let field_name_str = field_name.to_string();

            // Extract aliases from attributes
            let attributes = match extract_field_attributes(&field.attrs) {
                Ok(attributes) => attributes,
                Err(e) => return Some(Err(e)),
            };
            if attributes.skip {
                return None;
            }

            // Determine parser based on field type
            let (parser, is_option) = match determine_parser(&field.ty) {
                Ok((parser, is_option)) => (parser, is_option),
                Err(e) => return Some(Err(e)),
            };

            // Build the match conditions: field name and all aliases
            let mut match_conditions = vec![quote! { #field_name_str }];
            for alias in attributes.aliases {
                match_conditions.push(quote! { #alias });
            }

            let match_arm = if is_option {
                quote! {
                    #(#match_conditions)|* => self.#field_name = Some(#parser(v)?),
                }
            } else {
                quote! {
                    #(#match_conditions)|* => self.#field_name = #parser(v)?,
                }
            };

            Some(Ok(match_arm))
        })
        .try_collect()?;

    let env_setters = generate_load_from_env(fields)?;

    Ok(quote! {
        #[automatically_derived]
        impl crate::logstore::config::TryUpdateKey for #name {
            fn try_update_key(&mut self, key: &str, v: &str) -> crate::DeltaResult<Option<()>> {
                match key {
                    #(#match_arms)*
                    _ => return Ok(None),
                }
                Ok(Some(()))
            }

            fn load_from_environment(&mut self) -> crate::DeltaResult<()> {
                let default_values = Self::default();
                #(#env_setters)*
                Ok(())
            }
        }
    })
}

fn generate_load_from_env(fields: &[&Field]) -> Result<Vec<proc_macro2::TokenStream>> {
    fields.iter().filter_map(|field| {
        let field_name = &field.ident.as_ref().unwrap();
        let attributes = match extract_field_attributes(&field.attrs) {
            Ok(attributes) => attributes,
            Err(e) => return Some(Err(e)),
        };
        if attributes.skip || attributes.env_variable_names.is_empty() {
            return None;
        }

        let (parser, is_option) = match determine_parser(&field.ty) {
            Ok((parser, is_option)) => (parser, is_option),
            Err(e) => return Some(Err(e))
        };

        let env_checks = attributes.env_variable_names.iter().map(|env_var| {
            if is_option {
                // For Option types, only set if None
                quote! {
                    if self.#field_name.is_none() {
                        if let Ok(val) = std::env::var(#env_var) {
                            match #parser(&val) {
                                Ok(parsed) => self.#field_name = Some(parsed),
                                Err(e) => ::tracing::warn!("Failed to parse environment variable {}: {}", #env_var, e),
                            }
                        }
                    }
                }
            } else {
                // For non-Option types, we override the default value
                // but ignore it if the current value is not the default.
                quote! {
                    if self.#field_name == default_values.#field_name {
                        if let Ok(val) = std::env::var(#env_var) {
                            match #parser(&val) {
                                Ok(parsed) => self.#field_name = parsed,
                                Err(e) => ::tracing::warn!("Failed to parse environment variable {}: {}", #env_var, e),
                            }
                        }
                    }
                }
            }
        });

        Some(Ok(quote! {
            #(#env_checks)*
        }))
    }).try_collect()
}

// Helper function to determine the appropriate parser based on field type
fn determine_parser(ty: &Type) -> Result<(proc_macro2::TokenStream, bool)> {
    match ty {
        Type::Path(type_path) => {
            let type_str = quote! { #type_path }.to_string();
            let is_option = type_str.starts_with("Option");

            let caller = if type_str.contains("usize") {
                quote! { crate::logstore::config::parse_usize }
            } else if type_str.contains("f64") || type_str.contains("f32") {
                quote! { crate::logstore::config::parse_f64 }
            } else if type_str.contains("Duration") {
                quote! { crate::logstore::config::parse_duration }
            } else if type_str.contains("bool") {
                quote! { crate::logstore::config::parse_bool }
            } else if type_str.contains("String") {
                quote! { crate::logstore::config::parse_string }
            } else {
                return Err(Error::new_spanned(ty,
                    format!("Unsupported field type: {type_str}. Consider implementing a custom parser.")
                ));
            };

            Ok((caller, is_option))
        }
        _ => panic!("Unsupported field type for TryUpdateKey"),
    }
}

struct FieldAttributes {
    aliases: Vec<String>,
    env_variable_names: Vec<String>,
    docs: Option<String>,
    skip: bool,
}

/// Extract aliases from field attributes
///
/// Parse the annotations from individual fields into a convenient structure.
/// The field is annotated via `#[delta(...)]`. The following attributes are supported:
/// - `alias = "alias_name"`: Specifies an alias for the field.
/// - `env = "env_name"`: Specifies an environment variable name for the field.
/// - `skip`: Specifies whether the field should be skipped during parsing.
fn extract_field_attributes(attrs: &[Attribute]) -> Result<FieldAttributes> {
    let mut aliases = Vec::new();
    let mut environments = Vec::new();
    let mut docs = None;
    let mut doc_strings = Vec::new();
    let mut skip = false;

    for attr in attrs {
        if attr.path().is_ident("doc") {
            // Handle doc comments
            let meta = attr.meta.require_name_value()?;
            if let Expr::Lit(expr_lit) = &meta.value {
                if let Lit::Str(lit_str) = &expr_lit.lit {
                    // Collect all doc strings - they might span multiple lines
                    doc_strings.push(lit_str.value().trim().to_string());
                }
            }
        }
        if attr.path().is_ident("delta") {
            match &attr.meta {
                Meta::List(list) => {
                    let parser = Punctuated::<Meta, Token![,]>::parse_terminated;
                    let parsed = parser.parse(list.tokens.clone().into())?;

                    for val in parsed {
                        match val {
                            Meta::NameValue(MetaNameValue { path, value, .. }) => {
                                if path.is_ident("alias") {
                                    if let Expr::Lit(lit_expr) = &value {
                                        if let Lit::Str(lit_str) = &lit_expr.lit {
                                            aliases.push(lit_str.value());
                                        }
                                    }
                                }
                                if path.is_ident("environment") || path.is_ident("env") {
                                    if let Expr::Lit(lit_expr) = &value {
                                        if let Lit::Str(lit_str) = &lit_expr.lit {
                                            environments.push(lit_str.value());
                                        }
                                    }
                                }
                            }
                            Meta::Path(path) => {
                                if path.is_ident("skip") {
                                    skip = true;
                                }
                            }
                            _ => {
                                return Err(Error::new_spanned(
                                    &attr.meta,
                                    "only NameValue and Path parameters are supported",
                                ));
                            }
                        }
                    }
                }
                _ => {
                    return Err(Error::new_spanned(
                        &attr.meta,
                        "expected a list-style attribute",
                    ));
                }
            }
        }
    }

    // Combine all doc strings into a single documentation string
    if !doc_strings.is_empty() {
        docs = Some(doc_strings.join("\n"));
    }

    Ok(FieldAttributes {
        aliases,
        env_variable_names: environments,
        docs,
        skip,
    })
}
