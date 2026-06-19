<?php

namespace Utopia\Database\Replication;

/**
 * Forward-only cursor over a binary string.
 *
 * All integer reads are little-endian, matching the MySQL wire protocol. The
 * class is pure (no I/O), so it can be unit-tested against fixed byte fixtures.
 */
class BinaryReader
{
    private string $buffer;
    private int $position = 0;
    private int $length;

    public function __construct(string $buffer)
    {
        $this->buffer = $buffer;
        $this->length = \strlen($buffer);
    }

    public function eof(): bool
    {
        return $this->position >= $this->length;
    }

    public function remaining(): int
    {
        return $this->length - $this->position;
    }

    public function position(): int
    {
        return $this->position;
    }

    public function skip(int $bytes): void
    {
        $this->position += $bytes;
    }

    public function read(int $bytes): string
    {
        if ($bytes <= 0) {
            return '';
        }

        $value = \substr($this->buffer, $this->position, $bytes);
        $this->position += $bytes;

        return $value;
    }

    /**
     * Read a fixed-length little-endian unsigned integer (1-8 bytes).
     */
    public function readUInt(int $bytes): int
    {
        $value = 0;
        $chunk = $this->read($bytes);
        for ($i = 0; $i < $bytes; $i++) {
            $value |= \ord($chunk[$i]) << ($i * 8);
        }

        return $value;
    }

    public function readUInt8(): int
    {
        return \ord($this->read(1));
    }

    public function readUInt16(): int
    {
        return $this->readUInt(2);
    }

    public function readUInt32(): int
    {
        return $this->readUInt(4);
    }

    public function readUInt64(): int
    {
        return $this->readUInt(8);
    }

    /**
     * Read a length-encoded integer.
     *
     * @see https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_dt_integers.html
     */
    public function readLengthEncodedInt(): ?int
    {
        $first = $this->readUInt8();

        return match (true) {
            $first < 0xFB => $first,
            $first === 0xFB => null,        // NULL column
            $first === 0xFC => $this->readUInt(2),
            $first === 0xFD => $this->readUInt(3),
            default => $this->readUInt(8),  // 0xFE
        };
    }

    /**
     * Read a length-encoded string.
     */
    public function readLengthEncodedString(): ?string
    {
        $length = $this->readLengthEncodedInt();
        if ($length === null) {
            return null;
        }

        return $this->read($length);
    }

    /**
     * Read a NUL-terminated string.
     */
    public function readNullTerminatedString(): string
    {
        $end = \strpos($this->buffer, "\0", $this->position);
        if ($end === false) {
            $end = $this->length;
        }

        $value = \substr($this->buffer, $this->position, $end - $this->position);
        $this->position = $end + 1;

        return $value;
    }
}
