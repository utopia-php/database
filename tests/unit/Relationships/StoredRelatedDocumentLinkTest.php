<?php

namespace Tests\Unit\Relationships;

use Closure;
use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Hook\Relationships;
use Utopia\Database\Relationship;
use Utopia\Database\RelationshipDeleteAction;
use Utopia\Database\RelationshipType;
use Utopia\Database\Validator\Authorization;

/**
 * A stored document nested as a document in a new parent is updated and linked to the parent, whichever side
 * of the relationship holds the key.
 */
final class StoredRelatedDocumentLinkTest extends TestCase
{
    /**
     * @return iterable<string, array{Closure(): Adapter, RelationshipType, bool}>
     */
    public static function cases(): iterable
    {
        $adapters = [
            'memory' => static fn (): Adapter => new Memory(),
            'sqlite' => static fn (): Adapter => new SQLite(new PDO('sqlite::memory:')),
        ];

        foreach ($adapters as $adapterName => $adapter) {
            foreach ([RelationshipType::OneToOne, RelationshipType::OneToMany, RelationshipType::ManyToOne, RelationshipType::ManyToMany] as $type) {
                foreach (['single create' => false, 'batch create in a transaction' => true] as $writeName => $transaction) {
                    yield "{$adapterName}, {$type->value}, {$writeName}" => [$adapter, $type, $transaction];
                }
            }
        }
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('cases')]
    public function testAStoredRelatedDocumentSentAsADocumentIsUpdatedAndLinked(Closure $adapter, RelationshipType $type, bool $transaction): void
    {
        $authorization = new Authorization();
        $database = (new Database($adapter(), new Cache(new None())))
            ->setAuthorization($authorization)
            ->setDatabase('stored_link')
            ->setNamespace('stored_link_'.\uniqid());
        $database->addHook(new Permissions());
        $database->addHook(new Relationships());

        $authorization->skip(function () use ($database, $type, $transaction): void {
            $database->create();
            $database->createCollection(Collection::create('parents', attributes: [Attribute::string('name', size: 64)]));
            $database->createCollection(Collection::create('related', attributes: [Attribute::string('name', size: 64)]));
            $database->createRelationship('parents', match ($type) {
                RelationshipType::OneToOne => Relationship::oneToOne('related', 'related', twoWay: true, twoWayKey: 'back', onDelete: RelationshipDeleteAction::SetNull),
                RelationshipType::OneToMany => Relationship::oneToMany('related', 'related', twoWay: true, twoWayKey: 'back', onDelete: RelationshipDeleteAction::SetNull),
                RelationshipType::ManyToOne => Relationship::manyToOne('related', 'related', twoWay: true, twoWayKey: 'back', onDelete: RelationshipDeleteAction::SetNull),
                RelationshipType::ManyToMany => Relationship::manyToMany('related', 'related', twoWay: true, twoWayKey: 'back', onDelete: RelationshipDeleteAction::SetNull),
            });
            $database->createDocument('related', new Document(['$id' => 'stored', 'name' => 'Stored']));

            $related = new Document(['$id' => 'stored', 'name' => 'Changed']);
            $parent = new Document([
                '$id' => 'parent',
                'name' => 'Parent',
                'related' => \in_array($type, [RelationshipType::OneToOne, RelationshipType::ManyToOne], true) ? $related : [$related],
            ]);

            if ($transaction) {
                $database->withTransaction(fn (): int => $database->createDocuments('parents', [$parent]));
            } else {
                $database->createDocument('parents', $parent);
            }

            $stored = $database->getDocument('related', 'stored');
            $this->assertSame('Changed', $stored->getAttribute('name'), 'The stored document is updated');
            $this->assertSame(['parent'], self::ids($stored->getAttribute('back')), 'The stored document points back at its new parent');
            $this->assertSame(['stored'], self::ids($database->getDocument('parents', 'parent')->getAttribute('related')), 'The parent holds the stored document');
        });
    }

    /**
     * @return list<string>
     */
    private static function ids(mixed $value): array
    {
        $values = \is_array($value) ? $value : ($value === null ? [] : [$value]);

        return \array_values(\array_map(static fn (mixed $item): string => $item instanceof Document ? $item->getId() : (string) $item, $values));
    }
}
