<?php

namespace Tests\Unit\Seeder;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Seeder\Fixture;

class FixtureTest extends TestCase
{
    private Database $db;

    private Fixture $fixture;

    protected function setUp(): void
    {
        $this->db = $this->createMock(Database::class);
        $this->fixture = new Fixture();
    }

    public function testLoadCreatesDocumentsViaCreateDocument(): void
    {
        $this->db->expects($this->once())
            ->method('createDocument')
            ->with('users', $this->isInstanceOf(Document::class))
            ->willReturn(new Document(['$id' => 'u1', 'name' => 'Alice']));

        $this->fixture->load($this->db, 'users', [
            ['name' => 'Alice'],
        ]);

        $this->assertCount(1, $this->fixture->getCreated());
    }

    public function testLoadTracksCreatedIDs(): void
    {
        $this->db->method('createDocument')
            ->willReturnOnConsecutiveCalls(
                new Document(['$id' => 'u1', 'name' => 'Alice']),
                new Document(['$id' => 'u2', 'name' => 'Bob']),
            );

        $this->fixture->load($this->db, 'users', [
            ['name' => 'Alice'],
            ['name' => 'Bob'],
        ]);

        $created = $this->fixture->getCreated();
        $this->assertCount(2, $created);
        $this->assertEquals('u1', $created[0]['id']);
        $this->assertEquals('u2', $created[1]['id']);
    }

    public function testGetCreatedReturnsAllTrackedEntries(): void
    {
        $this->db->method('createDocument')
            ->willReturn(new Document(['$id' => 'doc1']));

        $this->fixture->load($this->db, 'users', [['name' => 'A']]);
        $this->fixture->load($this->db, 'posts', [['title' => 'B']]);

        $created = $this->fixture->getCreated();
        $this->assertCount(2, $created);
        $this->assertEquals('users', $created[0]['collection']);
        $this->assertEquals('posts', $created[1]['collection']);
    }

    public function testCleanupDeletesInReverseOrder(): void
    {
        $deleteOrder = [];

        $this->db->method('createDocument')
            ->willReturnOnConsecutiveCalls(
                new Document(['$id' => 'u1']),
                new Document(['$id' => 'u2']),
                new Document(['$id' => 'u3']),
            );

        $this->db->method('deleteDocument')
            ->willReturnCallback(function (string $collection, string $id) use (&$deleteOrder) {
                $deleteOrder[] = $id;

                return true;
            });

        $this->fixture->load($this->db, 'users', [
            ['name' => 'A'],
            ['name' => 'B'],
            ['name' => 'C'],
        ]);

        $this->fixture->cleanup($this->db);

        $this->assertEquals(['u3', 'u2', 'u1'], $deleteOrder);
    }

    public function testCleanupClearsTheCreatedList(): void
    {
        $this->db->method('createDocument')
            ->willReturn(new Document(['$id' => 'u1']));
        $this->db->method('deleteDocument')
            ->willReturn(true);

        $this->fixture->load($this->db, 'users', [['name' => 'A']]);
        $this->assertNotEmpty($this->fixture->getCreated());

        $this->fixture->cleanup($this->db);
        $this->assertEmpty($this->fixture->getCreated());
    }

    public function testCleanupHandlesDeleteErrorsSilently(): void
    {
        $this->db->method('createDocument')
            ->willReturn(new Document(['$id' => 'u1']));
        $this->db->method('deleteDocument')
            ->willThrowException(new \RuntimeException('Delete failed'));

        $this->fixture->load($this->db, 'users', [['name' => 'A']]);
        $this->fixture->cleanup($this->db);

        $this->assertEmpty($this->fixture->getCreated());
    }

    public function testLoadWithMultipleDocuments(): void
    {
        $this->db->expects($this->exactly(3))
            ->method('createDocument')
            ->willReturnOnConsecutiveCalls(
                new Document(['$id' => 'u1']),
                new Document(['$id' => 'u2']),
                new Document(['$id' => 'u3']),
            );

        $this->fixture->load($this->db, 'users', [
            ['name' => 'Alice'],
            ['name' => 'Bob'],
            ['name' => 'Charlie'],
        ]);

        $this->assertCount(3, $this->fixture->getCreated());
    }

    public function testLoadWithMultipleCollections(): void
    {
        $this->db->method('createDocument')
            ->willReturnOnConsecutiveCalls(
                new Document(['$id' => 'u1']),
                new Document(['$id' => 'p1']),
            );

        $this->fixture->load($this->db, 'users', [['name' => 'Alice']]);
        $this->fixture->load($this->db, 'posts', [['title' => 'Hello']]);

        $created = $this->fixture->getCreated();
        $this->assertCount(2, $created);
        $this->assertEquals('users', $created[0]['collection']);
        $this->assertEquals('posts', $created[1]['collection']);
    }

    public function testCleanupWithNoCreatedDocuments(): void
    {
        $this->db->expects($this->never())->method('deleteDocument');
        $this->fixture->cleanup($this->db);
        $this->assertEmpty($this->fixture->getCreated());
    }

    public function testMultipleCleanupCallsAreIdempotent(): void
    {
        $this->db->method('createDocument')
            ->willReturn(new Document(['$id' => 'u1']));
        $this->db->expects($this->once())->method('deleteDocument');

        $this->fixture->load($this->db, 'users', [['name' => 'A']]);
        $this->fixture->cleanup($this->db);
        $this->fixture->cleanup($this->db);
    }
}
