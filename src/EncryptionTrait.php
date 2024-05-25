<?php

declare(strict_types=1);

namespace NestboxPHP\Nestbox;

trait EncryptionTrait
{
    /**
     * Personal Research
     * https://medium.com/@london.lingo.01/unlocking-the-power-of-php-encryption-secure-data-transmission-and-encryption-algorithms-c5ed7a2cb481
     * https://stackoverflow.com/questions/18616573/how-to-check-fips-140-2-support-in-openssl
     * https://paragonie.com/blog/2022/06/recap-our-contributions-more-secure-internet
     * https://github.com/supergnaw/cyphper/blob/main/cyphper_static.php
     * https://stackoverflow.com/questions/19031540/does-php-use-a-fips-140-compliant-rng-to-generate-session-ids
     * https://wiki.openssl.org/index.php/FIPS_mode()
     * https://crypto.stackexchange.com/questions/105840/ecdh-security-vs-type-of-elliptic-curve
     *  - X25519 and P256: about 128-bit, P384: 192-bit, X448: 224-bit, P521: approximately 256-bit
     */

    /**
     * FIPS Compliant Algorithms:
     *
     *  AES – 128-bit or higher
     *  RSA – 2048 bits or higher
     *  TDES/TDEA – triple-length keys
     *  DSA/D-H – 2048/224 bits or higher
     *  ECC – 224 bit or higher
     */

    /**
     * HMAC is obsolete, use RSASSA or ECDSA; possible alternatives:
     *  https://www.php.net/manual/en/function.openssl-sign.php
     *  https://www.php.net/manual/en/function.openssl-verify.php
     */

    protected array $encryptionAlgorithms = [];

    protected function valid_algorithm(string $algorithm): bool
    {
        return array_key_exists($algorithm, $this->encryptionAlgorithms);
    }

    public function generate_bytes(int $length, int $maxRetries = 5): string|bool
    {
        $bytes = openssl_random_pseudo_bytes(length: $length, strong_result: $strongResult);
        if (!$strongResult) {
            if (0 < $maxRetries) {
                return $this->generate_bytes($length, $maxRetries - 1);
            }
            return false;
        }
        return $bytes;
    }

    public function generate_hexadecimal(int $chars): string
    {
        return bin2hex($this->generate_bytes(length: $chars / 2));
    }

    protected function generate_iv(string $input): string
    {
        return "";
    }

    public function generate_passphrase(string $input): string
    {
        return "";
    }

    public static function hmac_sign($cipherText, $key): string
    {
        return hash_hmac('sha256', $cipherText, $key) . $cipherText;
    }

    public static function hmac_authenticate($signedCipherText, $key): bool
    {
        $hmac = substr($signedCipherText, 0, 64);
        $cipherText = substr($signedCipherText, 64);
        return hash_equals(hash_hmac('sha256', $cipherText, $key), $hmac);
    }

    public function encrypt(string $input, string $algorithm, string $passphrase, string $iv): string|bool
    {
        // verify encryption algorithm
        if (!$this->valid_algorithm($algorithm)) {
            return false;
        }

        // require an ecryption password
        if (empty(trim($passphrase))) {
            return false;
        }

        // trim the initialzation vector
        $iv = substr(hash(algo: 'sha512', data: $iv), offset: 0, length: $this->encryptionAlgorithms[$algorithm]);

        // encrypt the data
        return openssl_encrypt(data: $input, cipher_algo: $algorithm, passphrase: $passphrase, iv: $iv);
    }

    public function decrypt(string $input, string $algorithm, string $passphrase, string $iv): string|bool
    {
        // verify dencryption algorithm
        if (!$this->valid_algorithm($algorithm)) {
            return false;
        }

        // require a decryption password
        if (empty(trim($passphrase))) {
            return false;
        }

        // trim the initialzation vector
        $iv = substr(hash(algo: 'sha512', data: $iv), offset: 0, length: $this->encryptionAlgorithms[$algorithm]);

        // decrypt the data
        return openssl_decrypt(data: $input, cipher_algo: $algorithm, passphrase: $passphrase, iv: $iv);
    }
}
