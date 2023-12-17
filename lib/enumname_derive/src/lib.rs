use proc_macro::TokenStream;
use quote::quote;
use syn;

#[proc_macro_derive(EnumName)]
pub fn enumname_derive(input: TokenStream) -> TokenStream {
    // Construct a representation of Rust code as a syntax tree
    // that we can manipulate
    let ast = syn::parse(input).unwrap();

    // Build the trait implementation
    impl_enumname(&ast)
}

fn impl_enumname(ast: &syn::DeriveInput) -> TokenStream {
    let name = &ast.ident;
    let mut arms = Vec::new();
    match &ast.data {
        syn::Data::Enum(data_enum) => {
            for pair in data_enum.variants.pairs() {
                let variant = *pair.value();
                match variant.fields {
                    syn::Fields::Named(_) => panic!("EnumName does not work on enums with named fields"),
                    syn::Fields::Unnamed(_) => panic!("EnumName does not work on enums with unnamed fields"),
                    syn::Fields::Unit => (),
                }
                let ident = &variant.ident;
                arms.push(quote! {#name::#ident => write!(f, "{}", stringify!(#ident))});
            }
        }
        _ => panic!("EnumName only works on enums"),
    }
    let gen = quote! {
        impl std::fmt::Display for #name {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                match self {
                    #(#arms),*
                }
            }
        }
    };
    gen.into()
}
