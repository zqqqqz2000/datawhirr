use std::fmt;

#[derive(Debug)]
pub struct ParameterError {
    reason: String,
}
impl std::error::Error for ParameterError {}

impl fmt::Display for ParameterError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "parameter error: {}", self.reason)
    }
}
impl ParameterError {
    pub fn new(reason: &str) -> ParameterError {
        ParameterError {
            reason: reason.to_string(),
        }
    }
}

#[derive(Debug)]
pub struct ParseError {
    detail: String,
}
impl std::error::Error for ParseError {}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "parameter error: {}", self.detail)
    }
}
impl ParseError {
    pub fn new(reason: &str) -> Self {
        ParseError {
            detail: reason.to_string(),
        }
    }
}
