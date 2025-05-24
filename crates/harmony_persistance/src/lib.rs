use directories::ProjectDirs;
use redb::{Database, DatabaseError};

pub fn get_project_dir() -> ProjectDirs {
    ProjectDirs::from("com", "parzivale", "harmony").unwrap()
}

pub fn get_database() -> Result<Database, DatabaseError> {
    let database_path = get_project_dir().project_path().join("database");
    Database::create(database_path)
}
