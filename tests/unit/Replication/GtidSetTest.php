<?php

namespace Tests\Unit\Replication;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Replication\GtidSet;

class GtidSetTest extends TestCase
{
    private const string SID = '3e11fa47-71ca-11e1-9e33-c80aa9429562';

    public function testParseAndStringRoundTrip(): void
    {
        $set = new GtidSet(self::SID . ':1-5:7-9');

        $this->assertSame(self::SID . ':1-5:7-9', (string) $set);
    }

    public function testAddMergesAdjacentTransactions(): void
    {
        $set = new GtidSet(self::SID . ':1-5');
        $set->add(self::SID, 6); // adjacent => extends the interval
        $set->add(self::SID, 8); // gap => new interval

        $this->assertSame(self::SID . ':1-6:8', (string) $set);
    }

    public function testAddCollapsesGap(): void
    {
        $set = new GtidSet(self::SID . ':1-5:7-9');
        $set->add(self::SID, 6); // fills the gap, merging both intervals

        $this->assertSame(self::SID . ':1-9', (string) $set);
    }

    public function testEmptySet(): void
    {
        $set = new GtidSet();

        $this->assertTrue($set->isEmpty());
        $this->assertSame('', (string) $set);
        // n_sids = 0 encoded as 8 little-endian bytes.
        $this->assertSame(\pack('P', 0), $set->encode());
    }

    public function testEncodeUsesHalfOpenIntervals(): void
    {
        $set = new GtidSet(self::SID . ':1-5');

        $expected = \pack('P', 1)                                   // one sid
            . \hex2bin(\str_replace('-', '', self::SID))            // 16-byte uuid
            . \pack('P', 1)                                         // one interval
            . \pack('P', 1)                                         // start
            . \pack('P', 6);                                        // end = last + 1

        $this->assertSame($expected, $set->encode());
    }
}
