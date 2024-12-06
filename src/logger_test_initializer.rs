use std::env::set_var;
use std::sync::Once;

#[cfg(test)]
static INIT_LOGGER: Once = Once::new();

#[cfg(test)]
pub fn init_logger() {
    INIT_LOGGER.call_once(|| {
        unsafe {
            set_var("RUST_LOG", "TRACE");
        }
        pretty_env_logger::formatted_builder().is_test(true).init();
    });
}
