<?php

namespace Tests\Unit\Attributes;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Attribute;
use Utopia\Database\AttributeUpdate;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Format;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Mirror;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\Schema\ColumnType;

final class UpdateAttributeTest extends TestCase
{
    public function testNullFieldsKeepTheirValues(): void
    {
        $database = $this->database();

        $updated = $database->updateAttribute('items', 'name', new AttributeUpdate(size: 128));

        $this->assertSame(128, $updated->size);
        $this->assertSame('none', $updated->default);
        $this->assertFalse($updated->required);
        $this->assertSame(ColumnType::String, $updated->type);
        $this->assertSame($updated->toDocument()->getArrayCopy(), $this->stored($database, 'name')->toDocument()->getArrayCopy());
    }

    public function testANullDefaultClearsTheDefault(): void
    {
        $database = $this->database();

        $updated = $database->updateAttribute('items', 'name', new AttributeUpdate(default: null));

        $this->assertNull($updated->default);
        $this->assertNull($this->stored($database, 'name')->default);
    }

    public function testADefaultIsSet(): void
    {
        $database = $this->database();

        $database->updateAttribute('items', 'name', new AttributeUpdate(default: 'other'));

        $this->assertSame('other', $this->stored($database, 'name')->default);
    }

    public function testADefaultOnARequiredAttributeIsRefused(): void
    {
        $database = $this->database();

        try {
            $database->updateAttribute('items', 'code', new AttributeUpdate(default: 'x'));
            $this->fail('Expected a default on a required attribute to be refused');
        } catch (DatabaseException $error) {
            $this->assertSame('Cannot set a default value on a required attribute', $error->getMessage());
        }

        $this->assertNull($this->stored($database, 'code')->default);
    }

    public function testARequiredAttributeTakesADefaultWhenMadeOptionalInTheSameUpdate(): void
    {
        $database = $this->database();

        $updated = $database->updateAttribute('items', 'code', new AttributeUpdate(required: false, default: 'x'));

        $this->assertFalse($updated->required);
        $this->assertSame('x', $this->stored($database, 'code')->default);
    }

    public function testMakingAnAttributeRequiredClearsItsDefault(): void
    {
        $database = $this->database();

        $updated = $database->updateAttribute('items', 'name', new AttributeUpdate(required: true));

        $this->assertTrue($updated->required);
        $this->assertNull($updated->default);
        $this->assertNull($this->stored($database, 'name')->default);
    }

    public function testMakingAnAttributeOptionalRelaxesItsColumn(): void
    {
        $adapter = new class () extends Memory {
            /**
             * @var list<string>
             */
            public array $relaxed = [];

            #[\Override]
            public function relaxAttributeRequired(string $collection, string $id): bool
            {
                $this->relaxed[] = $collection.'.'.$id;

                return parent::relaxAttributeRequired($collection, $id);
            }
        };
        $database = $this->database($adapter);

        $database->updateAttribute('items', 'code', new AttributeUpdate(required: false));

        $this->assertSame(['items.code'], $adapter->relaxed);
        $this->assertFalse($this->stored($database, 'code')->required);

        $created = $database->createDocument('items', new Document([
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $this->assertNull($created->getAttribute('code'));
    }

    public function testFormatAndFiltersAreReplaced(): void
    {
        $database = $this->database();

        $database->updateAttribute('items', 'name', new AttributeUpdate(format: null, filters: ['json']));

        $stored = $this->stored($database, 'name');
        $this->assertNull($stored->format);
        $this->assertSame(['json'], $stored->filters);
    }

    public function testAnUnknownFormatIsRefused(): void
    {
        $this->expectException(DatabaseException::class);

        $this->database()->updateAttribute('items', 'name', new AttributeUpdate(format: new Format('not-registered')));
    }

    public function testAMissingAttributeIsNotFound(): void
    {
        $this->expectException(NotFoundException::class);

        $this->database()->updateAttribute('items', 'missing', new AttributeUpdate(required: true));
    }

    public function testAMissingCollectionIsNotFound(): void
    {
        $this->expectException(NotFoundException::class);

        $this->database()->updateAttribute('missing', 'name', new AttributeUpdate(required: true));
    }

    public function testAnEmptyUpdateChangesNothing(): void
    {
        $database = $this->database();
        $before = $this->stored($database, 'name')->toDocument()->getArrayCopy();

        $updated = $database->updateAttribute('items', 'name', new AttributeUpdate());

        $this->assertSame($before, $updated->toDocument()->getArrayCopy());
        $this->assertSame($before, $this->stored($database, 'name')->toDocument()->getArrayCopy());
    }

    public function testTheMirrorReplicatesAnUpdate(): void
    {
        $destination = new Database(new Memory(), new Cache(new None()));
        $mirror = new Mirror(new Database(new Memory(), new Cache(new None())), $destination);
        $mirror
            ->setAuthorization(new Authorization())
            ->setDatabase('update_attribute')
            ->setNamespace('update_attribute_'.\uniqid());
        $destination->setAuthorization(new Authorization());
        $mirror->create();
        $mirror->createCollection($this->items());

        $mirror->updateAttribute('items', 'code', new AttributeUpdate(required: false, default: 'x'));
        $mirror->updateAttribute('items', 'name', new AttributeUpdate(default: null));

        $this->assertSame('x', $this->stored($destination, 'code')->default);
        $this->assertFalse($this->stored($destination, 'code')->required);
        $this->assertNull($this->stored($destination, 'name')->default);
        $this->assertSame('x', $this->stored($mirror->getSource(), 'code')->default);
    }

    private function database(?Adapter $adapter = null): Database
    {
        $database = new Database($adapter ?? new Memory(), new Cache(new None()));
        $database
            ->setAuthorization(new Authorization())
            ->setDatabase('update_attribute')
            ->setNamespace('update_attribute_'.\uniqid());
        $database->create();
        $database->createCollection($this->items());

        return $database;
    }

    private function items(): Collection
    {
        return Collection::create(
            'items',
            attributes: [
                Attribute::string('name', 64, default: 'none'),
                Attribute::string('code', 16, required: true),
            ],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
            documentSecurity: true,
        );
    }

    private function stored(Database $database, string $key): Attribute
    {
        foreach ($database->getCollection('items')->attributes() as $attribute) {
            if ($attribute->key === $key) {
                return $attribute;
            }
        }

        $this->fail('Attribute '.$key.' is missing from the collection metadata');
    }
}
