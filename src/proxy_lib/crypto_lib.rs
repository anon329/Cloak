use openssl::symm::{encrypt_aead, decrypt_aead, Cipher};
use openssl::rand::rand_bytes;
use super::{OBJET_SIZE, OBJET_SIZE_PLAIN};

/// Encryptor provides authenticated encryption for ORAM objects using AES-128-GCM
/// Each encrypted object has the following structure:
/// - IV (16 bytes): Random initialization vector
/// - Tag (16 bytes): Authentication tag
/// - Ciphertext (OBJECT_SIZE_PLAIN bytes): Encrypted data
pub struct Encryptor {
    /// The cipher algorithm (AES-128-GCM)
    cipher: Cipher,
    /// Encryption key (16 bytes)
    key: Vec<u8>,
}

impl Encryptor {
    /// Creates a new encryptor instance with a fixed key
    /// Note: In production, the key should be securely generated and managed
    pub fn new() -> Encryptor {
        let cipher = Cipher::aes_128_gcm();
        let key = b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0A\x0B\x0C\x0D\x0E\x0F".to_vec();
        Encryptor { cipher, key }
    }

    /// Encrypts data into a pre-allocated result buffer
    /// Format: [IV (16 bytes)][Tag (16 bytes)][Ciphertext]
    /// - Generates random IV
    /// - Encrypts data with AES-128-GCM
    /// - Writes IV, tag, and ciphertext to result buffer
    pub fn encrypt_object_in(&self, data: &[u8], result: &mut [u8]) {
        let mut iv = vec![0u8; 16];
        rand_bytes(&mut iv).unwrap();
        let mut tag = vec![0u8; 16];
        let chipertext = encrypt_aead(self.cipher, &self.key, Some(&iv), &[0u8], data, &mut tag).unwrap();
        result[..16].copy_from_slice(&iv);
        result[16..32].copy_from_slice(&tag);
        result[32..].copy_from_slice(&chipertext);
    }
    
    /// Encrypts data and returns a new array containing the encrypted object
    /// Same format as encrypt_object_in but allocates its own result buffer
    pub fn encrypt_object(&self, data: &[u8]) -> [u8; OBJET_SIZE] {
        let mut iv = vec![0u8; 16];
        rand_bytes(&mut iv).unwrap();
        let mut tag = vec![0u8; 16];
        let chipertext = encrypt_aead(self.cipher, &self.key, Some(&iv), &[0u8], data, &mut tag).unwrap();
        let mut result = [0u8; OBJET_SIZE];
        result[..16].copy_from_slice(&iv);
        result[16..32].copy_from_slice(&tag);
        result[32..].copy_from_slice(&chipertext);
        result
    }

    /// Decrypts an encrypted object into a pre-allocated result buffer
    /// - Extracts IV and authentication tag
    /// - Verifies authenticity and decrypts
    /// - Writes plaintext to result buffer
    pub fn decrypt_object_in(&self, chipertext: &[u8], result: &mut [u8]){
        let iv = &chipertext[..16];
        let tag = &chipertext[16..32];
        let chipertext = &chipertext[32..];
        let plaintext = decrypt_aead(self.cipher, &self.key, Some(iv), &[0u8], chipertext, tag).unwrap();
        result.copy_from_slice(&plaintext);
    }

    /// Decrypts an encrypted object and returns a new array containing the plaintext
    /// Same operation as decrypt_object_in but allocates its own result buffer
    pub fn decrypt_object(&self, chipertext: &[u8]) -> [u8; OBJET_SIZE_PLAIN] {
        let iv = &chipertext[..16];
        let tag = &chipertext[16..32];
        let chipertext = &chipertext[32..];
        let plaintext = decrypt_aead(self.cipher, &self.key, Some(iv), &[0u8], chipertext, tag).unwrap();
        let mut result = [0u8; OBJET_SIZE_PLAIN];
        result.copy_from_slice(&plaintext);
        result
    }
}