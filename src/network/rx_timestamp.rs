use std::io::{self, Read};
use std::time::{Instant, SystemTime};

use mio::net::TcpStream;

use crate::event::ReceiveTiming;
use crate::latency::{Latency, unix_ns};

pub struct Receiver {
    next_batch: u64,
    #[cfg(target_os = "linux")]
    kernel_timestamps: bool,
}

#[derive(Clone, Copy, Debug)]
pub struct ReadStamp {
    pub timing: ReceiveTiming,
}

struct SocketRead {
    count: usize,
    kernel_rx_unix_ns: Option<u64>,
    read_at: Instant,
    read_unix_ns: Option<u64>,
}

impl SocketRead {
    fn observe(count: usize) -> Self {
        Self {
            count,
            kernel_rx_unix_ns: None,
            read_at: Instant::now(),
            read_unix_ns: unix_ns(SystemTime::now()),
        }
    }
}

impl Receiver {
    pub fn new(_socket: &TcpStream) -> Self {
        Self {
            next_batch: 1,
            #[cfg(target_os = "linux")]
            kernel_timestamps: linux::enable(_socket),
        }
    }

    pub fn read(
        &mut self,
        socket: &mut TcpStream,
        bytes: &mut [u8],
        clock: &Latency,
    ) -> io::Result<(usize, ReadStamp)> {
        #[cfg(target_os = "linux")]
        let sample = if self.kernel_timestamps {
            linux::read(socket, bytes)?
        } else {
            SocketRead::observe(socket.read(bytes)?)
        };
        #[cfg(not(target_os = "linux"))]
        let sample = SocketRead::observe(socket.read(bytes)?);
        let stamp = ReadStamp {
            timing: ReceiveTiming {
                batch_id: self.next_batch,
                kernel_rx_unix_ns: sample.kernel_rx_unix_ns,
                read_unix_ns: sample.read_unix_ns,
                read_elapsed_ns: clock.elapsed_ns(sample.read_at),
                decoded_elapsed_ns: 0,
            },
        };
        self.next_batch += 1;
        Ok((sample.count, stamp))
    }
}

#[cfg(target_os = "linux")]
mod linux {
    use std::mem::{size_of, zeroed};
    use std::os::fd::AsRawFd;

    use super::*;

    pub fn enable(socket: &TcpStream) -> bool {
        let flags =
            (libc::SOF_TIMESTAMPING_RX_SOFTWARE | libc::SOF_TIMESTAMPING_SOFTWARE) as libc::c_int;
        // SAFETY: the socket is live and the input points to a correctly sized c_int.
        unsafe {
            libc::setsockopt(
                socket.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_TIMESTAMPING,
                (&flags as *const libc::c_int).cast(),
                size_of::<libc::c_int>() as libc::socklen_t,
            ) == 0
        }
    }

    pub fn read(socket: &TcpStream, bytes: &mut [u8]) -> io::Result<SocketRead> {
        socket.try_io(|| {
            let mut data = libc::iovec {
                iov_base: bytes.as_mut_ptr().cast(),
                iov_len: bytes.len(),
            };
            // Use an aligned control buffer with room for the complete SCM_TIMESTAMPING payload.
            let mut control = [0_usize; 24];
            // SAFETY: a zeroed msghdr has valid null pointers for its optional fields.
            let mut message: libc::msghdr = unsafe { zeroed() };
            message.msg_iov = &mut data;
            message.msg_iovlen = 1;
            message.msg_control = control.as_mut_ptr().cast();
            message.msg_controllen = size_of::<[usize; 24]>() as _;
            // SAFETY: all buffers described by message are valid and exclusive for this call.
            let count =
                unsafe { libc::recvmsg(socket.as_raw_fd(), &mut message, libc::MSG_DONTWAIT) };
            if count < 0 {
                return Err(io::Error::last_os_error());
            }
            let mut sample = SocketRead::observe(count as usize);
            sample.kernel_rx_unix_ns = timestamp(&message);
            Ok(sample)
        })
    }

    fn timestamp(message: &libc::msghdr) -> Option<u64> {
        if message.msg_flags & libc::MSG_CTRUNC != 0 {
            return None;
        }
        // SAFETY: recvmsg initialized the aligned control buffer. The CMSG accessors enforce bounds.
        unsafe {
            let mut header = libc::CMSG_FIRSTHDR(message);
            while let Some(cmsg) = header.as_ref() {
                if cmsg.cmsg_level == libc::SOL_SOCKET
                    && cmsg.cmsg_type == libc::SO_TIMESTAMPING
                    && cmsg.cmsg_len >= libc::CMSG_LEN(size_of::<[libc::timespec; 3]>() as u32) as _
                {
                    let stamp =
                        std::ptr::read_unaligned(libc::CMSG_DATA(header).cast::<libc::timespec>());
                    if stamp.tv_sec <= 0 || !(0..1_000_000_000).contains(&stamp.tv_nsec) {
                        return None;
                    }
                    return (stamp.tv_sec as u64)
                        .checked_mul(1_000_000_000)?
                        .checked_add(stamp.tv_nsec as u64);
                }
                header = libc::CMSG_NXTHDR(message, header);
            }
        }
        None
    }
}
