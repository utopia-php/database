<?php

namespace Tests\Unit\Seeder;

use PHPUnit\Framework\Attributes\AllowMockObjectsWithoutExpectations;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Seeder\Fixture;

#[AllowMockObjectsWithoutExpectations]
class FixtureTest extends TestCase
{
    private Database $db;

    private Fixture $fixture;

    protected function setUp(): void
    {
        $this->db = $this->createMock(Database::class);
        $this->fixture = new Fixture();
    }

    public function testLoadSingleDocumentUsesCreateDocument(): void
    {
        $this->db->expects($this->once())
            ->method('createDocument')
            ->with('users', $this->isInstanceOf(Document::class))
            ->willReturn(new Document(['$id' => 'u1', 'name' => 'Alice']));

        $this->fixture->load($this->db, 'users', [
            ['name' => 'Alice'],
        ]);

        $this->assertCount(1, $this->fixture->getCreated());
        $this->assertEquals('u1', $this->fixture->getCreated()[0]['id']);
    }

    public function testLoadMultipleDocumentsUsesCreateDocuments(): void
    {
        $this->db->expects($this->once())
            ->method('createDocuments')
            ->willReturnCallback(function (string $collection, array $docs, int $batch, ?callable $onNext) {
                foreach ($docs as $i => $doc) {
                    $created = new Document(['$id' => 'u' . ($i + 1)]);
                    if ($onNext) {
                        $onNext($created);
                    }
                }

                return \count($docs);
            });

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
            ->willReturnOnConsecutiveCalls(
                new Document(['$id' => 'doc1']),
                new Document(['$id' => 'doc2']),
            );

        $this->fixture->load($this->db, 'users', [['name' => 'A']]);
        $this->fixture->load($this->db, 'posts', [['title' => 'B']]);

        $created = $this->fixture->getCreated();
        $this->assertCount(2, $created);
        $this->assertEquals('users', $created[0]['collection']);
        $this->assertEquals('posts', $created[1]['collection']);
    }

    public function testCleanupDeletesDocumentsIndividually(): void
    {
        $this->db->method('createDocument')
            ->willReturn(new Document(['$id' => 'u1']));

        $this->db->expects($this->once())
            ->method('deleteDocument')
            ->with('users', 'u1')
            ->willReturn(true);

        $this->fixture->load($this->db, 'users', [['name' => 'A']]);
        $this->fixture->cleanup($this->db);

        $this->assertEmpty($this->fixture->getCreated());
    }

    public function testCleanupHandlesDeleteErrors(): void
    {
        $this->db->method('createDocument')
            ->willReturn(new Document(['$id' => 'u1']));
        $this->db->method('deleteDocument')
            ->willThrowException(new \RuntimeException('Delete failed'));

        $this->fixture->load($this->db, 'users', [['name' => 'A']]);
        $this->fixture->cleanup($this->db);

        $this->assertEmpty($this->fixture->getCreated());
    }

    public function testLoadWithEmptyArray(): void
    {
        $this->db->expects($this->never())->method('createDocument');
        $this->db->expects($this->never())->method('createDocuments');

        $this->fixture->load($this->db, 'users', []);
        $this->assertEmpty($this->fixture->getCreated());
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
        $this->db->expects($this->once())->method('deleteDocument')
            ->with('users', 'u1')
            ->willReturn(true);

        $this->fixture->load($this->db, 'users', [['name' => 'A']]);
        $this->fixture->cleanup($this->db);
        $this->fixture->cleanup($this->db);
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
}
