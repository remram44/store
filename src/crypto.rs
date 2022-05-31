//! Custom crypto for client -> storage messaging
//!
//! This is custom crypto code and I am sorry. It is unfortunate that this is
//! currently here. I would rather use a third-party solution here, however I
//! don't want to do multiple roundtrips to send a request.
//!
//! This implementation does not establish a channel with the storage daemon,
//! instead it uses key material shared by the master server to secure requests
//! to the storage daemons.

use aes::Aes128Enc;
use aes::cipher::{BlockEncrypt, KeyInit};
use aes::cipher::generic_array::GenericArray;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use hmac::{Hmac, Mac};
use log::warn;
use sha2::Sha256;
use std::io::Cursor;

/// A pair of keys: MAC and symmetric encryption
///
/// Currently using HMAC-SHA256 and AES128.
pub struct KeyPair {
    pub mac_key: [u8; 16],
    pub encrypt_key: [u8; 16],
}

const SIZE: usize = 16;
const MAC_SIZE: usize = 32;

fn cipher_block(cipher: &Aes128Enc, counter: u32) -> [u8; SIZE] {
    let mut block = [0; SIZE];
    block[0] = counter as u8;
    block[1] = (counter >> 8) as u8;
    block[2] = (counter >> 16) as u8;
    block[3] = (counter >> 24) as u8;
    let mut block = GenericArray::from(block);
    cipher.encrypt_block(&mut block);
    block.into()
}

fn xor_block(a: &mut [u8], b: &[u8]) {
    for (a1, b1) in a.into_iter().zip(b) {
        *a1 ^= b1;
    }
}

impl KeyPair {
    pub fn generate() -> KeyPair {
        todo!()
    }

    /// Encrypt and authenticate some data.
    ///
    /// The function takes the current counter value, and returns the new
    /// value. That counter is used to prevent replay attacks; messages will be
    /// rejected if it ever goes down.
    pub fn encrypt(&self, data: &[u8], counter: u32) -> (Vec<u8>, u32) {
        let mut result = Vec::new();
        let counter = self.encrypt_into(data, &mut result, counter);
        (result, counter)
    }

    /// Encrypt and authenticate some data.
    ///
    /// The function takes the current counter value, and returns the new
    /// value. That counter is used to prevent replay attacks; messages will be
    /// rejected if it ever goes down.
    pub fn encrypt_into(&self, data: &[u8], result: &mut Vec<u8>, mut counter: u32) -> u32 {
        result.clear();

        // Initialize cipher
        let cipher = Aes128Enc::new(&GenericArray::from(self.encrypt_key.clone()));

        // Write initial counter
        result.write_u32::<BigEndian>(counter).unwrap();

        // Prepare first block
        let mut block = [0u8; SIZE];
        // Write length
        Cursor::new(&mut block[..]).write_u32::<BigEndian>(data.len() as u32).unwrap();
        // Rest of block
        let rest = data.len().min(SIZE - 4);
        block[4..4 + rest].clone_from_slice(&data[0..rest]);

        // Encrypt
        xor_block(&mut block, &cipher_block(&cipher, counter));
        counter += 1;
        result.extend_from_slice(&block);
        let mut pos = rest;

        // Do other blocks
        while pos < data.len() {
            let rest = (data.len() - pos).min(SIZE);
            let mut block = [0; 16];
            block[0..rest].clone_from_slice(&data[pos..pos + rest]);
            xor_block(&mut block, &cipher_block(&cipher, counter));
            counter += 1;
            result.extend_from_slice(&block);
            pos += rest;
        }

        // Now add message digest
        let mut mac = <Hmac::<Sha256> as Mac>::new_from_slice(&self.mac_key).unwrap();
        mac.update(&result);
        let mac: [u8; MAC_SIZE] = mac.finalize().into_bytes().into();
        result.extend_from_slice(&mac);

        counter
    }

    /// Authenticate and decrypt some data
    ///
    /// The function takes the current counter value, and returns the new
    /// value. That counter is used to prevent replay attacks; if the message
    /// countains a counter too low, it will be rejected.
    pub fn decrypt(&self, data: &[u8], min_counter: u32) -> Option<(Vec<u8>, u32)> {
        let mut result = Vec::new();
        let counter = self.decrypt_into(data, &mut result, min_counter);
        counter.map(|c| (result, c))
    }

    /// Authenticate and decrypt some data
    ///
    /// The function takes the current counter value, and returns the new
    /// value. That counter is used to prevent replay attacks; if the message
    /// countains a counter too low, it will be rejected.
    pub fn decrypt_into(&self, data: &[u8], result: &mut Vec<u8>, min_counter: u32) -> Option<u32> {
        result.clear();

        if data.len() < 4 + SIZE + MAC_SIZE {
            warn!("decrypt: missing MAC (size={})", data.len());
            return None;
        }
        if data.len() % SIZE != (4 + MAC_SIZE) % SIZE {
            warn!("decrypt: wrong size ({})", data.len());
            return None;
        }

        // Check MAC
        let mut mac = <Hmac::<Sha256> as Mac>::new_from_slice(&self.mac_key).unwrap();
        mac.update(&data[0..data.len() - MAC_SIZE]);
        match mac.verify_slice(&data[data.len() - MAC_SIZE..]) {
            Ok(()) => {}
            Err(_) => {
                warn!("Invalid MAC");
                return None;
            }
        }

        // Read counter
        let mut counter = Cursor::new(&data).read_u32::<BigEndian>().unwrap();
        if counter < min_counter {
            warn!("Invalid counter");
            return None;
        }

        // Initialize cipher
        let cipher = Aes128Enc::new(&GenericArray::from(self.encrypt_key.clone()));

        // Prepare first block
        let mut block = [0u8; SIZE];
        block.clone_from_slice(&data[4..4 + SIZE]);

        // Decrypt
        xor_block(&mut block, &cipher_block(&cipher, counter));
        counter += 1;

        // Read total length
        let mut length = Cursor::new(&block).read_u32::<BigEndian>().unwrap() as usize;
        let copy_len = length.min(SIZE - 4);
        result.extend_from_slice(&block[4..4 + copy_len]);
        length -= copy_len;
        let mut pos = 4 + SIZE;

        // Do other blocks
        while length > 0 {
            let mut block = [0; SIZE];
            block.clone_from_slice(&data[pos..pos + SIZE]);
            pos += SIZE;
            xor_block(&mut block, &cipher_block(&cipher, counter));
            counter += 1;
            let copy_len = length.min(SIZE);
            result.extend_from_slice(&block[0..copy_len]);
            length -= copy_len;
        }

        if data.len() - pos != MAC_SIZE {
            warn!("Invalid size left over");
            return None;
        }

        Some(counter)
    }
}

#[cfg(test)]
mod tests {
    use super::{KeyPair, MAC_SIZE, SIZE};

    #[test]
    fn test_encrypt() {
        let message = b"\
            Lorem ipsum dolor sit amet, consectetur adipiscing elit. Maecenas \
            est purus, sagittis eu cursus sed, ullamcorper sed nibh. Mauris \
            quis aliquam leo. Integer porttitor sapien orci, sed semper ex \
            elementum maximus.";
        assert_eq!(message.len(), 211);
        let key_pair = KeyPair {
            mac_key: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            encrypt_key: [2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32],
        };
        let (result, counter) = key_pair.encrypt(message, 4);

        // Counter should increase by 14
        assert_eq!(counter, 18);

        // Result should be 14 blocks + counter + digest
        assert_eq!(result.len(), 4 + 14 * SIZE + MAC_SIZE);

        let mut expected = Vec::new();
        // Initial counter (4)
        expected.extend_from_slice(&[0, 0, 0, 4]);
        // Encrypted data
        expected.extend_from_slice(b"\
            \x6c\x25\xf2\x89\x66\xb2\x4b\x30\x72\x96\xf5\xb6\x76\xdc\x76\x41\
            \x16\xda\x5a\x77\x54\xee\xc3\x2c\x59\x09\xe4\x2f\x7c\x95\x4e\xf0\
            \xe5\xa7\xbc\xed\x59\x42\xdb\x7c\xcf\x63\x6a\x01\x98\x18\x73\xce\
            \x69\x36\x8c\x4a\xb5\x7c\xe3\xfb\x8d\xc6\x78\x68\x3b\x4a\x18\xde\
            \x82\x16\x2d\x5a\x38\xb9\xa4\x13\x17\x68\xf7\x16\xe0\x12\x7b\x60\
            \xde\x82\x8a\x0c\x31\x58\x19\x8e\x62\xa8\xa8\xc6\x4b\x72\xb1\xbb\
            \xf8\x77\xff\xcf\xa2\xf7\xa1\x21\xb7\xa5\x8e\x64\x8b\x5f\xe5\x6b\
            \x49\xf9\x14\xc8\xb5\x4d\x6e\x1a\x87\xb6\x27\x65\xf6\x8c\xfe\x33\
            \xc9\x4a\x25\xeb\x9b\x15\xc5\xb8\x6b\xd0\x1f\x60\xc2\x84\x33\x4b\
            \xd3\x43\xbb\x76\xda\x05\x53\xb2\x3c\x0f\x6f\x4c\x34\x7c\x4c\xbd\
            \x57\x90\x60\xf7\xbe\x1f\x0f\xa4\x7d\xc4\xb2\x5d\x88\x59\x37\x60\
            \x4e\x11\x9f\x0e\x77\xbf\x1f\xb1\x5a\xc9\xed\x3f\xde\xdc\xf4\x07\
            \x6c\xec\xbd\xa9\xe8\x7d\x8f\xfe\x81\x78\xa4\xdf\x4a\xc9\x6d\x49\
            \xdc\x15\x11\x95\x68\x40\xde\x9b\x6e\xe9\x1b\xc2\xda\xe4\x74\x2b");
        // MAC
        expected.extend_from_slice(b"\
          \xf5\x4d\x3c\xa0\x76\x5d\xef\xab\x12\x5b\xe1\x6f\x62\x6b\x85\x20\
          \x82\x50\xc5\x55\x89\xe4\x13\xc0\x86\x1a\x8c\xf4\x2d\xa7\x3f\xd4");
        assert_eq!(
            result,
            expected,
        );
    }

    #[test]
    fn test_decrypt() {
        let key_pair = KeyPair {
            mac_key: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            encrypt_key: [2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32],
        };

        let mut ciphertext = Vec::new();
        // Initial counter (4)
        ciphertext.extend_from_slice(&[0, 0, 0, 4]);
        // Encrypted data
        ciphertext.extend_from_slice(b"\
            \x6c\x25\xf2\x89\x66\xb2\x4b\x30\x72\x96\xf5\xb6\x76\xdc\x76\x41\
            \x16\xda\x5a\x77\x54\xee\xc3\x2c\x59\x09\xe4\x2f\x7c\x95\x4e\xf0\
            \xe5\xa7\xbc\xed\x59\x42\xdb\x7c\xcf\x63\x6a\x01\x98\x18\x73\xce\
            \x69\x36\x8c\x4a\xb5\x7c\xe3\xfb\x8d\xc6\x78\x68\x3b\x4a\x18\xde\
            \x82\x16\x2d\x5a\x38\xb9\xa4\x13\x17\x68\xf7\x16\xe0\x12\x7b\x60\
            \xde\x82\x8a\x0c\x31\x58\x19\x8e\x62\xa8\xa8\xc6\x4b\x72\xb1\xbb\
            \xf8\x77\xff\xcf\xa2\xf7\xa1\x21\xb7\xa5\x8e\x64\x8b\x5f\xe5\x6b\
            \x49\xf9\x14\xc8\xb5\x4d\x6e\x1a\x87\xb6\x27\x65\xf6\x8c\xfe\x33\
            \xc9\x4a\x25\xeb\x9b\x15\xc5\xb8\x6b\xd0\x1f\x60\xc2\x84\x33\x4b\
            \xd3\x43\xbb\x76\xda\x05\x53\xb2\x3c\x0f\x6f\x4c\x34\x7c\x4c\xbd\
            \x57\x90\x60\xf7\xbe\x1f\x0f\xa4\x7d\xc4\xb2\x5d\x88\x59\x37\x60\
            \x4e\x11\x9f\x0e\x77\xbf\x1f\xb1\x5a\xc9\xed\x3f\xde\xdc\xf4\x07\
            \x6c\xec\xbd\xa9\xe8\x7d\x8f\xfe\x81\x78\xa4\xdf\x4a\xc9\x6d\x49\
            \xdc\x15\x11\x95\x68\x40\xde\x9b\x6e\xe9\x1b\xc2\xda\xe4\x74\x2b");
        // MAC
        ciphertext.extend_from_slice(b"\
          \xf5\x4d\x3c\xa0\x76\x5d\xef\xab\x12\x5b\xe1\x6f\x62\x6b\x85\x20\
          \x82\x50\xc5\x55\x89\xe4\x13\xc0\x86\x1a\x8c\xf4\x2d\xa7\x3f\xd4");

        let (result, counter) = key_pair.decrypt(&ciphertext, 3).unwrap();
        assert_eq!(counter, 18);

        let message = b"\
            Lorem ipsum dolor sit amet, consectetur adipiscing elit. Maecenas \
            est purus, sagittis eu cursus sed, ullamcorper sed nibh. Mauris \
            quis aliquam leo. Integer porttitor sapien orci, sed semper ex \
            elementum maximus.";
        assert_eq!(result, message);
    }
}
