<?php

namespace Tests\Unit\Attributes;

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
use Utopia\Database\AttributeUpdate;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Exception\Refused as RefusedException;
use Utopia\Database\Permission;
use Utopia\Database\Role;
use Utopia\Database\Validator\Authorization;

final class RelaxRequiredTest extends TestCase
{
    public function testFailedRelaxLeavesTheAttributeRequired(): void
    {
        $database = $this->database(new class () extends Memory {
            #[\Override]
            public function relaxAttributeRequired(string $collection, string $id): bool
            {
                throw new DatabaseException('Relaxing the column failed');
            }
        });

        try {
            $database->updateAttribute('items', 'name', new AttributeUpdate(required: false));
            $this->fail('Expected the failed relax to surface');
        } catch (DatabaseException $error) {
            $this->assertSame('Relaxing the column failed', $error->getMessage());
        }

        $this->assertTrue($this->storedAttribute($database, 'name')->required);
    }

    public function testUnconfirmedRelaxLeavesTheAttributeRequired(): void
    {
        $database = $this->database(new class () extends Memory {
            #[\Override]
            public function relaxAttributeRequired(string $collection, string $id): bool
            {
                return false;
            }
        });

        try {
            $database->updateAttribute('items', 'name', new AttributeUpdate(required: false));
            $this->fail('Expected the unconfirmed relax to surface');
        } catch (RefusedException $error) {
            $this->assertSame('Failed to update attribute', $error->getMessage());
        }

        $this->assertTrue($this->storedAttribute($database, 'name')->required);
    }

    /**
     * @return array<string, array{Closure(): Adapter}>
     */
    public static function adapters(): array
    {
        return [
            'memory' => [static fn (): Adapter => new Memory()],
            'sqlite' => [static fn (): Adapter => new SQLite(new PDO('sqlite::memory:'))],
        ];
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testRelaxedAttributeAcceptsANull(Closure $adapter): void
    {
        $database = $this->database($adapter());

        $updated = $database->updateAttribute('items', 'name', new AttributeUpdate(required: false));

        $this->assertFalse($updated->required);
        $this->assertFalse($this->storedAttribute($database, 'name')->required);

        $created = $database->createDocument('items', new Document([
            Document::ID => 'nameless',
            Document::PERMISSIONS => [Permission::read(Role::any())],
            'name' => null,
        ]));

        $this->assertSame('nameless', $created->getId());
        $this->assertNull($database->getDocument('items', 'nameless')->getAttribute('name'));
    }

    private function database(Adapter $adapter): Database
    {
        $database = new Database($adapter, new Cache(new None()));
        $database
            ->setAuthorization(new Authorization())
            ->setDatabase('relax_required')
            ->setNamespace('relax_required_'.\uniqid());
        $database->create();
        $database->createCollection(Collection::create(
            id: 'items',
            attributes: [Attribute::string(key: 'name', size: 64, required: true)],
        ));

        return $database;
    }

    private function storedAttribute(Database $database, string $key): Attribute
    {
        foreach ($database->getCollection('items')->attributes() as $attribute) {
            if ($attribute->key === $key) {
                return $attribute;
            }
        }

        $this->fail('Attribute '.$key.' is missing from the collection metadata');
    }
}
