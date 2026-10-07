<?php

namespace Tests\Unit\Relationships;

use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Relationship as RelationshipException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Hook\Relationships;
use Utopia\Database\Relationship;
use Utopia\Database\RelationshipDeleteAction;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\Schema\ForeignKeyAction;

final class DeleteActionTest extends TestCase
{
    /**
     * @return array<string, array{ForeignKeyAction}>
     */
    public static function unsupportedActions(): array
    {
        return [
            'setDefault' => [ForeignKeyAction::SetDefault],
            'noAction' => [ForeignKeyAction::NoAction],
        ];
    }

    #[DataProvider('unsupportedActions')]
    public function testDeleteActionCannotExpressUnsupportedAction(ForeignKeyAction $action): void
    {
        $this->assertNull(RelationshipDeleteAction::tryFrom($action->value));
    }

    #[DataProvider('unsupportedActions')]
    public function testStoredUnsupportedActionIsRejectedFromArray(ForeignKeyAction $action): void
    {
        $this->expectException(RelationshipException::class);
        $this->expectExceptionMessage('"'.$action->value.'"');

        Relationship::fromArray(['relatedCollection' => 'albums', 'relationType' => 'oneToMany', 'twoWay' => true, 'twoWayKey' => 'artist', 'onDelete' => $action->value]);
    }

    #[DataProvider('unsupportedActions')]
    public function testStoredUnsupportedActionIsRejectedFromDocument(ForeignKeyAction $action): void
    {
        $this->expectException(RelationshipException::class);
        $this->expectExceptionMessage('"'.$action->value.'"');

        Relationship::fromDocument(new Document(['relatedCollection' => 'albums', 'relationType' => 'oneToMany', 'twoWay' => true, 'twoWayKey' => 'artist', 'onDelete' => $action->value]));
    }

    #[DataProvider('unsupportedActions')]
    public function testUnsupportedForeignKeyActionIsRejected(ForeignKeyAction $action): void
    {
        $this->expectException(RelationshipException::class);

        Relationship::fromArray(['relatedCollection' => 'albums', 'relationType' => 'oneToMany', 'onDelete' => $action]);
    }

    /**
     * @return array<string, array{RelationshipDeleteAction, ForeignKeyAction}>
     */
    public static function supportedActions(): array
    {
        return [
            'cascade' => [RelationshipDeleteAction::Cascade, ForeignKeyAction::Cascade],
            'restrict' => [RelationshipDeleteAction::Restrict, ForeignKeyAction::Restrict],
            'setNull' => [RelationshipDeleteAction::SetNull, ForeignKeyAction::SetNull],
        ];
    }

    #[DataProvider('supportedActions')]
    public function testSupportedActionMapsToItsForeignKeyAction(RelationshipDeleteAction $action, ForeignKeyAction $foreignKeyAction): void
    {
        $this->assertSame($foreignKeyAction, $action->toForeignKeyAction());
        $this->assertSame($action->value, $foreignKeyAction->value);
    }

    #[DataProvider('supportedActions')]
    public function testStoredSupportedActionHydrates(RelationshipDeleteAction $action, ForeignKeyAction $foreignKeyAction): void
    {
        $fromValue = Relationship::fromArray(['relatedCollection' => 'albums', 'relationType' => 'oneToMany', 'onDelete' => $action->value]);
        $fromForeignKeyAction = Relationship::fromArray(['relatedCollection' => 'albums', 'relationType' => 'oneToMany', 'onDelete' => $foreignKeyAction]);

        $this->assertSame($action, $fromValue->onDelete);
        $this->assertSame($action, $fromForeignKeyAction->onDelete);
    }

    #[DataProvider('unsupportedActions')]
    public function testDeletingParentWithStoredUnsupportedActionThrowsAndLeavesChildrenLinked(ForeignKeyAction $action): void
    {
        $database = $this->database();
        $database->createRelationship('artists', Relationship::oneToMany('albums', key: 'albums', twoWay: true, twoWayKey: 'artist', onDelete: RelationshipDeleteAction::Cascade));
        $database->createDocument('albums', new Document(['$id' => 'b1']));
        $database->createDocument('artists', new Document(['$id' => 'a1', 'albums' => ['b1']]));

        $this->storeDeleteAction($database, 'artists', 'albums', $action->value);

        $thrown = null;
        try {
            $database->deleteDocument('artists', 'a1');
        } catch (RelationshipException $exception) {
            $thrown = $exception;
        }

        $this->assertInstanceOf(RelationshipException::class, $thrown, 'Deleting a parent whose relationship stores onDelete "'.$action->value.'" must throw');

        $this->storeDeleteAction($database, 'artists', 'albums', RelationshipDeleteAction::Cascade->value);

        $artist = $database->skipRelationships(fn (): Document => $database->getDocument('artists', 'a1'));
        $album = $database->skipRelationships(fn (): Document => $database->getDocument('albums', 'b1'));

        $this->assertFalse($artist->isEmpty(), 'The parent survives the refused delete');
        $this->assertFalse($album->isEmpty(), 'The child survives the refused delete');
        $this->assertSame('a1', $album->getAttribute('artist'), 'The child stays linked to its parent');
    }

    #[DataProvider('unsupportedActions')]
    public function testCreatingACollectionWithAnUnsupportedActionIsRefused(ForeignKeyAction $action): void
    {
        $database = $this->database();
        $relationship = Relationship::oneToMany('albums', key: 'releases', twoWay: true, twoWayKey: 'artist')->toDocument()->getArrayCopy();
        $relationship['onDelete'] = $action->value;

        try {
            $database->createCollection(Collection::fromArray([
                '$id' => 'labels',
                'attributes' => [[
                    'key' => 'releases',
                    'type' => 'relationship',
                    'size' => 0,
                    'required' => false,
                    'signed' => true,
                    'array' => false,
                    'filters' => [],
                    'options' => [...$relationship, 'side' => 'parent'],
                ]],
            ]));
            $this->fail('Creating a collection whose relationship stores onDelete "'.$action->value.'" must throw');
        } catch (RelationshipException $exception) {
            $this->assertStringContainsString('"'.$action->value.'"', $exception->getMessage());
        }

        $this->assertNull($database->findCollection('labels'));
    }

    #[DataProvider('unsupportedActions')]
    public function testACollectionStoringAnUnsupportedActionCannotBeRead(ForeignKeyAction $action): void
    {
        $database = $this->database();
        $database->createRelationship('artists', Relationship::oneToMany('albums', key: 'albums', twoWay: true, twoWayKey: 'artist', onDelete: RelationshipDeleteAction::Cascade));
        $database->createDocument('albums', new Document(['$id' => 'b1']));
        $database->createDocument('artists', new Document(['$id' => 'a1', 'albums' => ['b1']]));

        $this->storeDeleteAction($database, 'artists', 'albums', $action->value);

        foreach ([
            'getCollection' => fn (): mixed => $database->getCollection('artists')->attributes(),
            'getDocument' => fn (): mixed => $database->getDocument('artists', 'a1'),
            'find' => fn (): mixed => $database->find('artists'),
        ] as $read => $callback) {
            try {
                $callback();
                $this->fail($read.' of a collection storing onDelete "'.$action->value.'" must throw');
            } catch (RelationshipException $exception) {
                $this->assertStringContainsString('"'.$action->value.'"', $exception->getMessage(), $read);
            }
        }
    }

    private function storeDeleteAction(Database $database, string $collection, string $key, string $action): void
    {
        $authorization = $database->getAuthorization();
        $metadata = $authorization->skip(fn (): Document => $database->getDocument(Database::METADATA, $collection));

        $attributes = [];
        /** @var array<Document|array<string, mixed>> $stored */
        $stored = $metadata->getAttribute('attributes', []);
        foreach ($stored as $attribute) {
            $attribute = $attribute instanceof Document ? $attribute : new Document($attribute);
            if ($attribute->getId() === $key) {
                $options = $attribute->getAttribute('options', []);
                /** @var array<string, mixed> $options */
                $options = $options instanceof Document ? $options->getArrayCopy() : $options;
                $options['onDelete'] = $action;
                $attribute->setAttribute('options', $options);
            }
            $attributes[] = $attribute;
        }
        $metadata->setAttribute('attributes', $attributes);

        $authorization->skip(fn (): Document => $database->silent(fn (): Document => $database->updateDocument(Database::METADATA, $collection, $metadata)));
        $database->purgeCachedCollection($collection);
    }

    private function database(): Database
    {
        $authorization = new Authorization();
        $authorization->addRole(Role::any()->toString());

        $database = new Database(new Memory(), new Cache(new None()));
        $database
            ->setAuthorization($authorization)
            ->setDatabase('delete_action')
            ->setNamespace('delete_action_'.\uniqid());
        $database->create();
        $database->addHook(new Relationships($database));
        $database->addHook(new Permissions());

        $permissions = [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ];
        $database->createCollection(Collection::create('artists', permissions: $permissions, documentSecurity: false));
        $database->createCollection(Collection::create('albums', permissions: $permissions, documentSecurity: false));

        return $database;
    }
}
