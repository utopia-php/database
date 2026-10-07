<?php

namespace Tests\Unit\Attributes;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Attribute;
use Utopia\Database\AttributeUpdate;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Index;
use Utopia\Database\Validator\Authorization;

final class RenamePathsTest extends TestCase
{
    public function testRenameAttributeRenames(): void
    {
        $database = $this->database();

        $database->renameAttribute('items', 'name', 'title');

        $this->assertRenamed($database);
    }

    public function testUpdateAttributeWithAKeyRenames(): void
    {
        $database = $this->database();

        $updated = $database->updateAttribute('items', 'name', new AttributeUpdate(key: 'title'));

        $this->assertSame('title', $updated->key);
        $this->assertRenamed($database);
    }

    public function testRenameAttributeRefusesAKeyInUse(): void
    {
        $database = $this->database();

        $this->expectException(DuplicateException::class);

        $database->renameAttribute('items', 'name', 'code');
    }

    public function testUpdateAttributeRefusesAKeyInUse(): void
    {
        $database = $this->database();

        $this->expectException(DuplicateException::class);

        $database->updateAttribute('items', 'name', new AttributeUpdate(key: 'code'));
    }

    public function testThereIsNoOtherPublicRenamePath(): void
    {
        $database = $this->database();

        foreach (['updateAttributeMeta', 'updateAttributeRequired', 'updateAttributeFormat', 'updateAttributeFormatOptions', 'updateAttributeFilters', 'updateAttributeDefault'] as $method) {
            $this->assertFalse(\is_callable([$database, $method]), $method.' must not be callable');
        }
    }

    private function assertRenamed(Database $database): void
    {
        $collection = $database->getCollection('items');
        $keys = \array_map(static fn (Attribute $attribute): string => $attribute->key, $collection->attributes());

        $this->assertSame(['title', 'code'], $keys);
        $this->assertSame(['title', 'code'], $collection->indexes()[0]->attributes);

        $document = $database->getDocument('items', 'first');
        $this->assertSame('First', $document->getAttribute('title'));
        $this->assertFalse($document->offsetExists('name'));
    }

    private function database(): Database
    {
        $database = new Database(new Memory(), new Cache(new None()));
        $database
            ->setAuthorization(new Authorization())
            ->setDatabase('rename_paths')
            ->setNamespace('rename_paths_'.\uniqid());
        $database->create();
        $database->createCollection(Collection::create(
            'items',
            attributes: [Attribute::string('name', 64), Attribute::string('code', 16)],
            indexes: [Index::key('name_code', ['name', 'code'])],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
        ));
        $database->createDocument('items', new Document([
            '$id' => 'first',
            '$permissions' => [Permission::read(Role::any())],
            'name' => 'First',
            'code' => 'f',
        ]));

        return $database;
    }
}
