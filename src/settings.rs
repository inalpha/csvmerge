use config::{ConfigError, Config, File};

#[derive(Debug, Deserialize)]
pub struct Column {
    pub label: String,
    pub matches: Vec<String>,
    #[serde(default)]
    pub unique: bool,
}

#[derive(Debug, Deserialize)]
pub struct Settings {
    pub columns: Vec<Column>
}

impl Settings {
    pub fn new() -> Result<Self, ConfigError> {
        let mut s = Config::new();
        s.merge(File::with_name("Settings"))?;
        s.try_into()
    }
}