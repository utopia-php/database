<?php

namespace Utopia\Database\Seeder;

use Faker\Factory as FakerFactory;
use Faker\Generator;
use Utopia\Database\Database;
use Utopia\Database\Document;

class Factory
{
    private Generator $faker;

    /** @var array<string, FactoryDefinition> */
    private array $definitions = [];

    public function __construct(?Generator $faker = null)
    {
        $this->faker = $faker ?? FakerFactory::create();
    }

    public function define(string $collection, callable $definition): void
    {
        $this->definitions[$collection] = new FactoryDefinition($definition);
    }

    /**
     * @param  array<string, mixed>  $overrides
     */
    public function make(string $collection, array $overrides = []): Document
    {
        if (! isset($this->definitions[$collection])) {
            throw new \RuntimeException("No factory defined for collection '{$collection}'");
        }

        /** @var array<string, mixed> $data */
        $data = ($this->definitions[$collection]->callback)($this->faker);

        return new Document(\array_merge($data, $overrides));
    }

    /**
     * @param  array<string, mixed>  $overrides
     * @return array<Document>
     */
    public function makeMany(string $collection, int $count, array $overrides = []): array
    {
        $documents = [];
        for ($i = 0; $i < $count; $i++) {
            $documents[] = $this->make($collection, $overrides);
        }

        return $documents;
    }

    /**
     * @param  array<string, mixed>  $overrides
     */
    public function create(string $collection, Database $db, array $overrides = []): Document
    {
        return $db->createDocument($collection, $this->make($collection, $overrides));
    }

    /**
     * @param  array<string, mixed>  $overrides
     * @return array<Document>
     */
    public function createMany(string $collection, Database $db, int $count, array $overrides = []): array
    {
        $documents = [];
        for ($i = 0; $i < $count; $i++) {
            $documents[] = $this->create($collection, $db, $overrides);
        }

        return $documents;
    }

    public function getFaker(): Generator
    {
        return $this->faker;
    }
}
