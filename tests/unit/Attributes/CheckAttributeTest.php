<?php

namespace Tests\Unit\Attributes;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Validator\Authorization;

final class CheckAttributeTest extends TestCase
{
    public function testAnAttributeWithinTheLimitPasses(): void
    {
        $database = $this->database([Attribute::string('first', 16), Attribute::string('second', 16), Attribute::string('third', 16)]);

        $this->assertTrue($database->checkAttribute('items', Attribute::string('fourth', 16)));
    }

    public function testAnAttributeOverTheLimitIsRefused(): void
    {
        $database = $this->database([Attribute::string('first', 16), Attribute::string('second', 16), Attribute::string('third', 16), Attribute::string('fourth', 16)]);

        $this->expectException(LimitException::class);

        $database->checkAttribute('items', Attribute::string('fifth', 16));
    }

    public function testCheckingDoesNotStoreTheAttribute(): void
    {
        $database = $this->database([Attribute::string('first', 16)]);

        $database->checkAttribute('items', Attribute::string('second', 16));

        $this->assertSame(['first'], \array_map(
            static fn (Attribute $attribute): string => $attribute->key,
            $database->getCollection('items')->attributes(),
        ));
    }

    public function testAMissingCollectionIsNotFound(): void
    {
        $this->expectException(NotFoundException::class);

        $this->database([])->checkAttribute('missing', Attribute::string('first', 16));
    }

    /**
     * @param  list<Attribute>  $attributes
     */
    private function database(array $attributes): Database
    {
        $adapter = new class () extends Memory {
            #[\Override]
            public function getLimitForAttributes(): int
            {
                return $this->getCountOfDefaultAttributes() + \count(Database::collectionDefinition()->attributes());
            }
        };
        $database = new Database($adapter, new Cache(new None()));
        $database
            ->setAuthorization(new Authorization())
            ->setDatabase('check_attribute')
            ->setNamespace('check_attribute_'.\uniqid());
        $database->create();
        $database->createCollection(Collection::create('items', attributes: $attributes));

        return $database;
    }
}
