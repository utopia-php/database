<?php

namespace Tests\Unit\Attributes;

use Closure;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Validator\Authorization;

/**
 * An adapter that reports a column write as not done, rather than throwing, leaves the collection's metadata as it was.
 */
final class AdapterRefusalTest extends TestCase
{
    private const string COLLECTION = 'items';

    public function testAnAttributeTheAdapterDidNotCreateIsAnErrorAndStaysOutOfTheMetadata(): void
    {
        $database = $this->database(createAttribute: static fn (): bool => false);

        try {
            $database->createAttribute(self::COLLECTION, Attribute::string(key: 'title', size: 64));
            $this->fail('An attribute the adapter did not create must be an error');
        } catch (DatabaseException $error) {
            $this->assertSame('Failed to create attribute', $error->getMessage());
        }

        $this->assertSame([], $database->getCollection(self::COLLECTION)->attributes);
    }

    /**
     * @param  (Closure(Attribute): bool)|null  $createAttribute
     * @param  (Closure(array<Attribute>): bool)|null  $createAttributes
     */
    private function database(?Closure $createAttribute = null, ?Closure $createAttributes = null): Database
    {
        $adapter = new class ($createAttribute, $createAttributes) extends Memory {
            /**
             * @param  (Closure(Attribute): bool)|null  $single
             * @param  (Closure(array<Attribute>): bool)|null  $batch
             */
            public function __construct(private readonly ?Closure $single, private readonly ?Closure $batch)
            {
                parent::__construct();
            }

            #[\Override]
            public function createAttribute(string $collection, Attribute $attribute): bool
            {
                return $this->single === null ? parent::createAttribute($collection, $attribute) : ($this->single)($attribute);
            }

            /**
             * @param  array<Attribute>  $attributes
             */
            #[\Override]
            public function createAttributes(string $collection, array $attributes): bool
            {
                return $this->batch === null ? parent::createAttributes($collection, $attributes) : ($this->batch)($attributes);
            }
        };

        $database = new Database($adapter, new Cache(new None()));
        $database
            ->setAuthorization(new Authorization())
            ->setDatabase('refusal')
            ->setNamespace('refusal_'.\uniqid());
        $database->create();
        $database->createCollection(new Collection(id: self::COLLECTION));

        return $database;
    }
}
