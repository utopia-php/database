<?php

namespace Tests\Unit\Loading;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Document;
use Utopia\Database\Event;
use Utopia\Database\Loading\NPlusOneDetector;

class NPlusOneDetectorTest extends TestCase
{
    public function testDetectsExcessiveQueries(): void
    {
        $detected = new \stdClass();
        $detected->value = false;
        $detector = new NPlusOneDetector(3, function () use ($detected) {
            $detected->value = true;
        });

        $doc = new Document(['$id' => '1', '$collection' => 'users']);

        $detector->handle(Event::DocumentFind, $doc);
        $detector->handle(Event::DocumentFind, $doc);
        $this->assertFalse($detected->value);

        $detector->handle(Event::DocumentFind, $doc);
        $this->assertNotEmpty($detector->getViolations());
        $this->assertTrue($detected->value);
    }

    public function testIgnoresNonQueryEvents(): void
    {
        $detector = new NPlusOneDetector(2);

        $detector->handle(Event::DocumentCreate, new Document(['$collection' => 'users']));
        $detector->handle(Event::DocumentCreate, new Document(['$collection' => 'users']));
        $detector->handle(Event::DocumentCreate, new Document(['$collection' => 'users']));

        $this->assertEmpty($detector->getViolations());
    }

    public function testGetQueryCounts(): void
    {
        $detector = new NPlusOneDetector(100);

        $detector->handle(Event::DocumentFind, new Document(['$collection' => 'users']));
        $detector->handle(Event::DocumentFind, new Document(['$collection' => 'users']));
        $detector->handle(Event::DocumentFind, new Document(['$collection' => 'posts']));

        $counts = $detector->getQueryCounts();
        $this->assertEquals(2, $counts['document_find:users']);
        $this->assertEquals(1, $counts['document_find:posts']);
    }

    public function testReset(): void
    {
        $detector = new NPlusOneDetector(5);

        $detector->handle(Event::DocumentFind, new Document(['$collection' => 'users']));
        $detector->reset();

        $this->assertEmpty($detector->getQueryCounts());
    }
}
