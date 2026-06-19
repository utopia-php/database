<?php

namespace Tests\Unit\Replication;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Replication\BinaryReader;

class BinaryReaderTest extends TestCase
{
    public function testFixedWidthIntegersAreLittleEndian(): void
    {
        $reader = new BinaryReader("\x01\x02\x00\x03\x00\x00\x00");

        $this->assertSame(1, $reader->readUInt8());
        $this->assertSame(2, $reader->readUInt16());
        $this->assertSame(3, $reader->readUInt32());
        $this->assertTrue($reader->eof());
    }

    public function testLengthEncodedInteger(): void
    {
        // 0xFA inline, 0xFC + 2 bytes, 0xFB => NULL.
        $reader = new BinaryReader("\xFA\xFC\x00\x01\xFB");

        $this->assertSame(0xFA, $reader->readLengthEncodedInt());
        $this->assertSame(256, $reader->readLengthEncodedInt());
        $this->assertNull($reader->readLengthEncodedInt());
    }

    public function testLengthEncodedString(): void
    {
        $reader = new BinaryReader("\x05hello\xFB");

        $this->assertSame('hello', $reader->readLengthEncodedString());
        $this->assertNull($reader->readLengthEncodedString());
    }

    public function testNullTerminatedString(): void
    {
        $reader = new BinaryReader("appwrite\x00rest");

        $this->assertSame('appwrite', $reader->readNullTerminatedString());
        $this->assertSame('rest', $reader->read($reader->remaining()));
    }

    public function testSkipAdvancesCursor(): void
    {
        $reader = new BinaryReader("\xAA\xBB\xCC");
        $reader->skip(2);

        $this->assertSame(0xCC, $reader->readUInt8());
    }
}
