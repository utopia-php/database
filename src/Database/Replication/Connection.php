<?php

namespace Utopia\Database\Replication;

use Swoole\Coroutine\Socket;
use Utopia\Database\Exception as DatabaseException;

/**
 * A coroutine MySQL connection speaking just enough of the client protocol to
 * authenticate (MySQL 8 caching_sha2_password, with mysql_native_password
 * fallback), run a handful of setup queries and request a binlog dump.
 *
 * Swoole-native: all socket I/O happens on a {@see Socket}, so it yields the
 * current coroutine instead of blocking the worker.
 */
class Connection
{
    private const int MAX_PACKET_SIZE = 0x40000000;
    private const int CHARSET_UTF8MB4 = 45;
    private const int PROTOCOL_TCP = 6; // IPPROTO_TCP

    private Socket $socket;
    private int $sequence = 0;

    public function __construct(
        private readonly string $host,
        private readonly int $port,
        private readonly string $username,
        private readonly string $password,
        private readonly bool $ssl = false,
        private readonly float $timeout = 30.0,
    ) {
    }

    public function connect(): void
    {
        $this->socket = new Socket(AF_INET, SOCK_STREAM, self::PROTOCOL_TCP);

        if (!$this->socket->connect($this->host, $this->port, $this->timeout)) {
            throw new DatabaseException("Failed to connect to {$this->host}:{$this->port}: {$this->socket->errMsg}");
        }

        $this->authenticate();
    }

    public function close(): void
    {
        if (isset($this->socket)) {
            $this->socket->close();
        }
    }

    /**
     * Read one logical protocol packet, transparently reassembling payloads
     * split across multiple 16MB frames.
     */
    public function readPacket(): string
    {
        $payload = '';

        do {
            $header = $this->socket->recvAll(4, $this->timeout);
            if ($header === false || \strlen($header) < 4) {
                throw new DatabaseException("Connection closed while reading packet header: {$this->socket->errMsg}");
            }

            $length = \ord($header[0]) | (\ord($header[1]) << 8) | (\ord($header[2]) << 16);
            $this->sequence = \ord($header[3]);

            if ($length > 0) {
                $chunk = $this->socket->recvAll($length, $this->timeout);
                if ($chunk === false || \strlen($chunk) < $length) {
                    throw new DatabaseException("Connection closed while reading packet body: {$this->socket->errMsg}");
                }
                $payload .= $chunk;
            }
        } while ($length === 0xFFFFFF);

        return $payload;
    }

    /**
     * Write a packet using the next sequence id, splitting oversized payloads.
     */
    public function writePacket(string $payload): void
    {
        $this->sequence++;
        $this->sendFrames($payload);
    }

    /**
     * Begin a new command, resetting the sequence id to 0.
     */
    public function writeCommand(string $payload): void
    {
        $this->sequence = -1;
        $this->writePacket($payload);
    }

    private function sendFrames(string $payload): void
    {
        $length = \strlen($payload);
        $offset = 0;

        do {
            $size = \min($length - $offset, 0xFFFFFF);
            $header = \chr($size & 0xFF) . \chr(($size >> 8) & 0xFF) . \chr(($size >> 16) & 0xFF) . \chr($this->sequence & 0xFF);
            if ($this->socket->sendAll($header . \substr($payload, $offset, $size)) === false) {
                throw new DatabaseException("Failed to write packet: {$this->socket->errMsg}");
            }
            $offset += $size;
            // Only advance the sequence between continuation frames; the caller
            // owns the increment for the next logical packet.
            if ($size === 0xFFFFFF) {
                $this->sequence++;
            }
        } while ($size === 0xFFFFFF);
    }

    /**
     * Run a simple statement and discard its result. Intended for SET commands.
     */
    public function execute(string $sql): void
    {
        $this->writeCommand(\chr(Constants::COM_QUERY) . $sql);
        $this->assertOk($this->readPacket());
    }

    /**
     * Run a query expected to return a single scalar (one row, one column).
     */
    public function queryScalar(string $sql): ?string
    {
        $this->writeCommand(\chr(Constants::COM_QUERY) . $sql);

        $first = $this->readPacket();
        $marker = \ord($first[0]);
        if ($marker === Constants::PACKET_ERR) {
            $this->throwError($first);
        }

        $reader = new BinaryReader($first);
        $columns = $reader->readLengthEncodedInt() ?? 0;

        for ($i = 0; $i < $columns; $i++) {
            $this->readPacket(); // column definition
        }
        $this->readPacket(); // EOF after column definitions

        $value = null;
        while (true) {
            $packet = $this->readPacket();
            $type = \ord($packet[0]);
            if ($type === Constants::PACKET_EOF && \strlen($packet) < 9) {
                break; // EOF / end of rows
            }
            if ($type === Constants::PACKET_ERR) {
                $this->throwError($packet);
            }

            $value ??= (new BinaryReader($packet))->readLengthEncodedString();
        }

        return $value;
    }

    /**
     * Read a packet and assert it is an OK packet (used after a command).
     */
    public function readOk(): void
    {
        $this->assertOk($this->readPacket());
    }

    /**
     * Throw if the given stream packet is an ERR packet.
     */
    public function throwIfError(string $packet): void
    {
        if (\ord($packet[0]) === Constants::PACKET_ERR) {
            $this->throwError($packet);
        }
    }

    private function authenticate(): void
    {
        $handshake = new BinaryReader($this->readPacket());

        $handshake->skip(1); // protocol version (10)
        $handshake->readNullTerminatedString(); // server version
        $handshake->skip(4); // connection id
        $authData = $handshake->read(8);
        $handshake->skip(1); // filler
        $capabilities = $handshake->readUInt16();
        $handshake->skip(1); // charset
        $handshake->skip(2); // status flags
        $capabilities |= $handshake->readUInt16() << 16;
        $authDataLen = $handshake->readUInt8();
        $handshake->skip(10); // reserved

        if ($capabilities & Constants::CLIENT_SECURE_CONNECTION) {
            $authData .= $handshake->read(\max(13, $authDataLen - 8));
        }

        $plugin = ($capabilities & Constants::CLIENT_PLUGIN_AUTH)
            ? $handshake->readNullTerminatedString()
            : 'mysql_native_password';

        $nonce = \substr($authData, 0, 20);

        if ($this->ssl) {
            $this->upgradeToTls($capabilities);
        }

        $this->sendHandshakeResponse($nonce, $plugin);
        $this->finishAuth($nonce, $plugin);
    }

    /**
     * Send the abbreviated SSL request packet and upgrade the socket to TLS,
     * before the credentials are sent. The full handshake response then travels
     * over the encrypted channel.
     */
    private function upgradeToTls(int $serverCapabilities): void
    {
        if (!($serverCapabilities & Constants::CLIENT_SSL)) {
            throw new DatabaseException('TLS requested but the server does not support it');
        }

        $payload = \pack('V', $this->clientCapabilities() | Constants::CLIENT_SSL)
            . \pack('V', self::MAX_PACKET_SIZE)
            . \chr(self::CHARSET_UTF8MB4)
            . \str_repeat("\0", 23);
        $this->writePacket($payload);

        $this->socket->setProtocol(['open_ssl' => true, 'ssl_verify_peer' => false]);
        if (!$this->socket->sslHandshake()) {
            throw new DatabaseException("TLS handshake failed: {$this->socket->errMsg}");
        }
    }

    private function clientCapabilities(): int
    {
        return Constants::CLIENT_LONG_PASSWORD
            | Constants::CLIENT_LONG_FLAG
            | Constants::CLIENT_PROTOCOL_41
            | Constants::CLIENT_SECURE_CONNECTION
            | Constants::CLIENT_PLUGIN_AUTH
            | Constants::CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA;
    }

    private function sendHandshakeResponse(string $nonce, string $plugin): void
    {
        $capabilities = $this->clientCapabilities();
        if ($this->ssl) {
            $capabilities |= Constants::CLIENT_SSL;
        }

        $authResponse = $this->scramble($plugin, $nonce);

        $payload = \pack('V', $capabilities)
            . \pack('V', self::MAX_PACKET_SIZE)
            . \chr(self::CHARSET_UTF8MB4)
            . \str_repeat("\0", 23)
            . $this->username . "\0"
            . $this->lengthEncodedInt(\strlen($authResponse)) . $authResponse
            . $plugin . "\0";

        $this->writePacket($payload);
    }

    private function finishAuth(string $nonce, string $plugin): void
    {
        while (true) {
            $packet = $this->readPacket();
            $marker = \ord($packet[0]);

            switch ($marker) {
                case Constants::PACKET_OK:
                    return;
                case Constants::PACKET_ERR:
                    $this->throwError($packet);
                    // no break — throwError always throws
                case Constants::PACKET_EOF:
                    // Auth switch request: re-scramble for the requested plugin.
                    $reader = new BinaryReader(\substr($packet, 1));
                    $plugin = $reader->readNullTerminatedString();
                    $nonce = \substr($reader->read($reader->remaining()), 0, 20);
                    $this->writePacket($this->scramble($plugin, $nonce));
                    break;
                case Constants::PACKET_AUTH_MORE_DATA:
                    $status = \ord($packet[1]);
                    if ($status === Constants::AUTH_FAST_SUCCESS) {
                        break; // OK packet follows
                    }
                    if ($status === Constants::AUTH_FULL_REQUIRED) {
                        $this->fullAuth($nonce);
                        break;
                    }
                    throw new DatabaseException('Unexpected auth status: ' . $status);
                default:
                    throw new DatabaseException('Unexpected auth packet marker: ' . $marker);
            }
        }
    }

    /**
     * caching_sha2_password full authentication over a plaintext channel:
     * fetch the server's RSA public key and send the password XOR-masked with
     * the nonce, encrypted with RSA-OAEP.
     */
    private function fullAuth(string $nonce): void
    {
        $this->writePacket(\chr(Constants::AUTH_REQUEST_PUBLIC_KEY));

        $packet = $this->readPacket();
        if (\ord($packet[0]) !== Constants::PACKET_AUTH_MORE_DATA) {
            throw new DatabaseException('Expected public key in auth exchange');
        }
        $publicKey = \substr($packet, 1);

        $plain = $this->password . "\0";
        $masked = '';
        $nonceLen = \strlen($nonce);
        for ($i = 0, $len = \strlen($plain); $i < $len; $i++) {
            $masked .= $plain[$i] ^ $nonce[$i % $nonceLen];
        }

        if (!\openssl_public_encrypt($masked, $encrypted, $publicKey, OPENSSL_PKCS1_OAEP_PADDING)) {
            throw new DatabaseException('Failed to RSA-encrypt credentials: ' . \openssl_error_string());
        }

        $this->writePacket($encrypted);
    }

    private function scramble(string $plugin, string $nonce): string
    {
        if ($this->password === '') {
            return '';
        }

        return match ($plugin) {
            'mysql_native_password' => $this->scrambleNative($nonce),
            default => $this->scrambleCachingSha2($nonce),
        };
    }

    private function scrambleCachingSha2(string $nonce): string
    {
        $m1 = \hash('sha256', $this->password, true);
        $m2 = \hash('sha256', $m1, true);
        $m3 = \hash('sha256', $m2 . $nonce, true);

        return $m1 ^ $m3;
    }

    private function scrambleNative(string $nonce): string
    {
        $stage1 = \sha1($this->password, true);
        $stage2 = \sha1($stage1, true);
        $token = \sha1($nonce . $stage2, true);

        return $stage1 ^ $token;
    }

    private function lengthEncodedInt(int $value): string
    {
        return match (true) {
            $value < 0xFB => \chr($value),
            $value < 0x10000 => \chr(0xFC) . \pack('v', $value),
            $value < 0x1000000 => \chr(0xFD) . \substr(\pack('V', $value), 0, 3),
            default => \chr(0xFE) . \pack('P', $value),
        };
    }

    private function assertOk(string $packet): void
    {
        $marker = \ord($packet[0]);
        if ($marker === Constants::PACKET_ERR) {
            $this->throwError($packet);
        }
    }

    private function throwError(string $packet): never
    {
        $reader = new BinaryReader(\substr($packet, 1));
        $code = $reader->readUInt16();
        $message = $reader->read($reader->remaining());
        // Skip the SQL-state marker ('#XXXXX') when present.
        if (\str_starts_with($message, '#')) {
            $message = \substr($message, 6);
        }

        throw new DatabaseException("MySQL error {$code}: {$message}");
    }
}
