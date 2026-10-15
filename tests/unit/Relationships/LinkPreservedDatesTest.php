<?php

namespace Tests\Unit\Relationships;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Hook\Relationships;
use Utopia\Database\Relationship;
use Utopia\Database\RelationshipDeleteAction;
use Utopia\Database\Validator\Authorization;

final class LinkPreservedDatesTest extends TestCase
{
    private const string STORED = '2020-01-01T00:00:00.000+00:00';

    public function testLinkingChildrenThroughTheirParentKeepsTheirUpdatedAtUnderPreservedDates(): void
    {
        [$database, $authorization] = $this->database();

        $authorization->skip(fn () => $database->withPreserveDates(true, fn () => $database->updateDocument('authors', 'author3', new Document(['books' => ['book4', 'book5']]))));

        foreach (['book4', 'book5'] as $id) {
            $book = $authorization->skip(fn () => $database->getDocument('books', $id));
            $author = $book->getAttribute('author');
            $this->assertInstanceOf(Document::class, $author, $id);
            $this->assertSame('author3', $author->getId(), $id);
            $this->assertSame(self::STORED, $book->getUpdatedAt(), $id);
        }
    }

    public function testLinkingWithoutPreservedDatesStillStampsTheChildren(): void
    {
        [$database, $authorization] = $this->database();

        $authorization->skip(fn () => $database->updateDocument('authors', 'author3', new Document(['books' => ['book4']])));

        $this->assertNotSame(self::STORED, $authorization->skip(fn () => $database->getDocument('books', 'book4'))->getUpdatedAt());
    }

    public function testUnlinkingUnderPreservedDatesStillStampsTheChild(): void
    {
        [$database, $authorization] = $this->database();
        $authorization->skip(fn () => $database->withPreserveDates(true, fn () => $database->updateDocument('authors', 'author3', new Document(['books' => ['book4']]))));

        $authorization->skip(fn () => $database->withPreserveDates(true, fn () => $database->updateDocument('authors', 'author3', new Document(['books' => []]))));

        $this->assertNotSame(self::STORED, $authorization->skip(fn () => $database->getDocument('books', 'book4'))->getUpdatedAt());
    }

    /**
     * @return array{Database, Authorization}
     */
    private function database(): array
    {
        $authorization = new Authorization();
        $database = (new Database(new Memory(), new Cache(new None())))
            ->setAuthorization($authorization)
            ->setDatabase('link_dates')
            ->setNamespace('link_dates_'.\uniqid());
        $database->addHook(new Relationships());

        $authorization->skip(function () use ($database): void {
            $database->create();
            $database->createCollection(Collection::create('authors', attributes: [Attribute::string('name', size: 64)]));
            $database->createCollection(Collection::create('books', attributes: [Attribute::string('name', size: 64)]));
            $database->createRelationship('authors', Relationship::oneToMany('books', 'books', twoWay: true, twoWayKey: 'author', onDelete: RelationshipDeleteAction::SetNull));
            $database->withPreserveDates(true, function () use ($database): void {
                $database->createDocument('authors', new Document(['$id' => 'author3', 'name' => 'Linus', '$createdAt' => self::STORED, '$updatedAt' => self::STORED]));
                foreach (['book4', 'book5'] as $id) {
                    $database->createDocument('books', new Document(['$id' => $id, 'name' => $id, '$createdAt' => self::STORED, '$updatedAt' => self::STORED]));
                }
            });
        });

        return [$database, $authorization];
    }
}
