use anyhow::{Result, bail};

use crate::maker::{Instrument, Intent};

pub struct Signer;

impl Signer {
    pub fn from_env(_: u64, _: u8) -> Result<Self> {
        bail!("the Lighter signer requires macOS or Linux")
    }

    pub fn auth(&self) -> Result<String> {
        bail!("the Lighter signer requires macOS or Linux")
    }

    pub fn sign(&self, _: &Intent, _: &Instrument, _: u64) -> Result<(String, String)> {
        bail!("the Lighter signer requires macOS or Linux")
    }
}
