// ABI from lighter-python v1.1.9 (signer_client.py).
use std::env;
use std::ffi::{CStr, CString, c_char, c_int, c_void};
use std::os::unix::ffi::OsStrExt;
use std::ptr::NonNull;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail, ensure};
use rust_decimal::{Decimal, prelude::ToPrimitive};
use serde_json::{Value, json};

use crate::intent::Side;
use crate::maker::{Action, Instrument, Intent, Kind};

#[repr(C)]
struct Strings {
    value: *mut c_char,
    error: *mut c_char,
}

#[repr(C)]
struct Signed {
    tx_type: u8,
    info: *mut c_char,
    hash: *mut c_char,
    message: *mut c_char,
    error: *mut c_char,
}

type Create = unsafe extern "C" fn(*const c_char, *const c_char, c_int, c_int, i64) -> *mut c_char;
type Auth = unsafe extern "C" fn(i64, c_int, i64) -> Strings;
type Place = unsafe extern "C" fn(
    i64,
    i64,
    i64,
    c_int,
    c_int,
    c_int,
    c_int,
    c_int,
    c_int,
    i64,
    i64,
    c_int,
    c_int,
    u8,
    u8,
    u8,
    i64,
    c_int,
    i64,
) -> Signed;
type Cancel = unsafe extern "C" fn(i64, i64, u8, i64, c_int, i64) -> Signed;
type Free = unsafe extern "C" fn(*mut c_void);

struct Library(NonNull<c_void>);

impl Library {
    fn open() -> Result<Self> {
        let path = env::var_os("LIGHTER_TESTNET_SIGNER_LIB")
            .context("set LIGHTER_TESTNET_SIGNER_LIB to the v1.1.9 official signer library")?;
        let path = CString::new(path.as_os_str().as_bytes())?;
        // The Go runtime starts background threads. Keep the image loaded until process exit.
        let library = unsafe {
            libc::dlopen(
                path.as_ptr(),
                libc::RTLD_NOW | libc::RTLD_LOCAL | libc::RTLD_NODELETE,
            )
        };
        NonNull::new(library)
            .map(Self)
            .context("could not load Lighter signer")
    }

    fn get<T: Copy>(&self, name: &CStr) -> Result<T> {
        let symbol = unsafe { libc::dlsym(self.0.as_ptr(), name.as_ptr()) };
        ensure!(!symbol.is_null(), "missing Lighter signer symbol {name:?}");
        // The caller supplies the exact C function signature for this symbol.
        Ok(unsafe { std::mem::transmute_copy(&symbol) })
    }
}

impl Drop for Library {
    fn drop(&mut self) {
        unsafe {
            libc::dlclose(self.0.as_ptr());
        }
    }
}

pub struct Signer {
    _library: Library,
    auth: Auth,
    place: Place,
    cancel: Cancel,
    free: Free,
    account: i64,
    key_index: c_int,
}

impl Signer {
    pub fn from_env(account: u64, key_index: u8) -> Result<Self> {
        ensure!(
            key_index >= 3,
            "use a dedicated Lighter testnet API key index (at least 3)"
        );
        let account = account.try_into()?;
        let secret = env::var("LIGHTER_TESTNET_API_KEY").context("set LIGHTER_TESTNET_API_KEY")?;
        Self::new(account, key_index, &secret)
    }

    fn new(account: i64, key_index: u8, secret: &str) -> Result<Self> {
        let secret = CString::new(secret.strip_prefix("0x").unwrap_or(secret))?;
        let library = Library::open()?;
        let create: Create = library.get(c"CreateClient")?;
        let signer = Self {
            auth: library.get(c"CreateAuthToken")?,
            place: library.get(c"SignCreateOrder")?,
            cancel: library.get(c"SignCancelOrder")?,
            free: library.get(c"Free")?,
            _library: library,
            account,
            key_index: key_index.into(),
        };
        // The current documented testnet endpoint uses Lighter chain 300.
        let error = unsafe {
            create(
                c"https://api.testnet.lighter.xyz".as_ptr(),
                secret.as_ptr(),
                300,
                signer.key_index,
                account,
            )
        };
        ensure!(
            signer.read(error).is_none(),
            "Lighter signer rejected the testnet key"
        );
        Ok(signer)
    }

    pub fn auth(&self) -> Result<String> {
        let seconds: i64 = SystemTime::now()
            .duration_since(UNIX_EPOCH)?
            .as_secs()
            .try_into()?;
        let result = unsafe { (self.auth)(seconds + 600, self.key_index, self.account) };
        let token = self.read(result.value);
        ensure!(
            self.read(result.error).is_none(),
            "could not create Lighter testnet auth token"
        );
        token.context("Lighter signer returned an empty auth token")
    }

    pub fn sign(&self, intent: &Intent, meta: &Instrument, nonce: u64) -> Result<(String, String)> {
        let nonce = nonce.try_into()?;
        let signed = match intent.action {
            Action::Place {
                side,
                kind: Kind::PostOnly,
                price,
                size,
            } => {
                let price: u32 = units(price, meta.price_decimals)?.try_into()?;
                let price = i32::from_ne_bytes(price.to_ne_bytes());
                let size = units(size, meta.size_decimals)?;
                let now: i64 = SystemTime::now()
                    .duration_since(UNIX_EPOCH)?
                    .as_millis()
                    .try_into()?;
                unsafe {
                    (self.place)(
                        meta.asset.into(),
                        intent.client.try_into()?,
                        size,
                        price,
                        i32::from(side == Side::Sell),
                        0,
                        2,
                        0,
                        0,
                        now + super::ORDER_LIFETIME_MS as i64,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        nonce,
                        self.key_index,
                        self.account,
                    )
                }
            }
            Action::Cancel { oid } => {
                // The signer accepts either the venue order index or the 48-bit client index.
                unsafe {
                    (self.cancel)(
                        meta.asset.into(),
                        oid.unwrap_or(intent.client).try_into()?,
                        0,
                        nonce,
                        self.key_index,
                        self.account,
                    )
                }
            }
            _ => bail!("unsupported Lighter action"),
        };
        let tx = self.read(signed.info);
        let hash = self.read(signed.hash);
        let _ = self.read(signed.message);
        ensure!(
            self.read(signed.error).is_none(),
            "Lighter signer rejected the order action"
        );
        let info: Value =
            serde_json::from_str(&tx.context("Lighter signer returned no transaction")?)
                .map_err(|_| anyhow::anyhow!("invalid Lighter signed transaction format"))?;
        Ok((
            json!({
                "type": "jsonapi/sendtx",
                "data": {"id": intent.id.to_string(), "tx_type": signed.tx_type, "tx_info": info}
            })
            .to_string(),
            hash.context("Lighter signer returned no transaction hash")?,
        ))
    }

    fn read(&self, ptr: *mut c_char) -> Option<String> {
        if ptr.is_null() {
            return None;
        }
        let output = unsafe { CStr::from_ptr(ptr).to_string_lossy().into_owned() };
        unsafe {
            (self.free)(ptr.cast());
        }
        Some(output)
    }
}

fn units(value: Decimal, decimals: u32) -> Result<i64> {
    let scaled = value * Decimal::from(10_i64.pow(decimals));
    ensure!(scaled.fract().is_zero(), "Lighter order is off tick or lot");
    scaled
        .to_i64()
        .context("Lighter order is outside signer integer bounds")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore = "requires the official Lighter signer v1.1.9 shared library"]
    fn signs_with_the_official_lighter_library_without_submitting() -> Result<()> {
        #[repr(C)]
        struct Key {
            private: *mut c_char,
            public: *mut c_char,
            error: *mut c_char,
        }
        type Generate = unsafe extern "C" fn() -> Key;
        let library = Library::open()?;
        let generate: Generate = library.get(c"GenerateAPIKey")?;
        let free: Free = library.get(c"Free")?;
        let response = unsafe { generate() };
        let private = (!response.private.is_null()).then(|| {
            unsafe { CStr::from_ptr(response.private) }
                .to_string_lossy()
                .into_owned()
        });
        for ptr in [response.private, response.public, response.error] {
            if !ptr.is_null() {
                unsafe { free(ptr.cast()) };
            }
        }
        let signer = Signer::new(12345, 3, &private.context("missing generated signer key")?)?;
        assert!(!signer.auth()?.is_empty());
        let meta = Instrument {
            asset: 4096,
            size_decimals: 5,
            price_decimals: 1,
            min_size: Decimal::new(1, 5),
            min_notional: Decimal::TEN,
            maker_fee: Decimal::ZERO,
            taker_fee: Decimal::ZERO,
        };
        let mut intent = Intent {
            id: 1,
            caused_by_seq: 2,
            ordinal: 0,
            venue: crate::maker::Venue::Lighter,
            client: 1 << 40,
            action: Action::Place {
                side: Side::Buy,
                kind: Kind::PostOnly,
                price: Decimal::new(90_000, 0),
                size: Decimal::new(3, 4),
            },
        };
        let (wire, hash) = signer.sign(&intent, &meta, 10)?;
        let order: Value = serde_json::from_str(&wire)?;
        assert!(!hash.is_empty());
        assert_eq!(order["type"], "jsonapi/sendtx");
        assert_eq!(order["data"]["tx_info"]["ClientOrderIndex"], 1_u64 << 40);
        assert_eq!(order["data"]["tx_info"]["TimeInForce"], 2);
        assert_eq!(order["data"]["tx_info"]["Nonce"], 10);
        assert_eq!(order["data"]["tx_info"]["Price"], 900_000);
        intent.action = Action::Cancel { oid: None };
        let (wire, _) = signer.sign(&intent, &meta, 11)?;
        let cancel: Value = serde_json::from_str(&wire)?;
        assert_eq!(cancel["data"]["tx_info"]["Index"], 1_u64 << 40);
        assert_eq!(cancel["data"]["tx_info"]["Nonce"], 11);
        Ok(())
    }
}
