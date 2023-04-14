use std::fmt::Display;

static LOGGER: LogLevel = LogLevel {
    logger: Logger::DEBUG,
};
#[derive(PartialEq)]
pub enum Logger {
    DEBUG,
    INFO,
    WARN,
    ERROR,
}

pub struct LogLevel {
    pub logger: Logger,
}

impl Logger {
    pub fn error<T: Display>(msg: T) {
        eprintln!("{msg}");
    }
    pub fn warn<T: Display>(msg: T) {
        if LOGGER.logger == Logger::ERROR {
            return;
        }
        eprintln!("{msg}");
    }
    pub fn info<T: Display>(msg: T) {
        if LOGGER.logger == Logger::ERROR || LOGGER.logger == Logger::WARN {
            return;
        }
        eprintln!("{msg}");
    }
    pub fn debug<T: Display>(msg: T) {
        if LOGGER.logger == Logger::ERROR
            || LOGGER.logger == Logger::WARN
            || LOGGER.logger == Logger::INFO
        {
            return;
        }
        eprintln!("{msg}");
    }
}
