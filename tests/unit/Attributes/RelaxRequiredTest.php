<?php

namespace Tests\Unit\Attributes;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Exception as DatabaseException;
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
            $database->updateAttribute('items', 'name', required: false);
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
            $database->updateAttribute('items', 'name', required: false);
            $this->fail('Expected the unconfirmed relax to surface');
        } catch (DatabaseException $error) {
            $this->assertSame('Failed to update attribute', $error->getMessage());
        }

        $this->assertTrue($this->storedAttribute($database, 'name')->required);
    }

    public function testRequiredOnlyChangeRelaxesWithoutRewritingTheColumn(): void
    {
        $adapter = new class () extends Memory {
            /**
             * @var list<string>
             */
            public array $calls = [];

            #[\Override]
            public function relaxAttributeRequired(string $collection, string $id): bool
            {
                $this->calls[] = 'relax '.$id;

                return parent::relaxAttributeRequired($collection, $id);
            }

            #[\Override]
            public function updateAttribute(string $collection, Attribute $attribute, ?string $newKey = null): bool
            {
                $this->calls[] = 'update '.$attribute->key;

                return parent::updateAttribute($collection, $attribute, $newKey);
            }
        };
        $database = $this->database($adapter);

        $updated = $database->updateAttribute('items', 'name', required: false);

        $this->assertFalse($updated->getAttribute('required'));
        $this->assertFalse($this->storedAttribute($database, 'name')->required);
        $this->assertSame(['relax name'], $adapter->calls);
    }

    private function database(Adapter $adapter): Database
    {
        $database = new Database($adapter, new Cache(new None()));
        $database
            ->setAuthorization(new Authorization())
            ->setDatabase('relax_required')
            ->setNamespace('relax_required_'.\uniqid());
        $database->create();
        $database->createCollection(new Collection(
            id: 'items',
            attributes: [Attribute::string(key: 'name', size: 64, required: true)],
        ));

        return $database;
    }

    private function storedAttribute(Database $database, string $key): Attribute
    {
        foreach ($database->getCollection('items')->attributes as $attribute) {
            if ($attribute->key === $key) {
                return $attribute;
            }
        }

        $this->fail('Attribute '.$key.' is missing from the collection metadata');
    }
}
