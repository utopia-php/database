<?php

namespace Tests\Unit;

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
use Utopia\Database\Validator\Authorization;

final class LegacyAttributeFormatTest extends TestCase
{
    /**
     * @return array<string, array{Closure(): Adapter}>
     */
    public static function adapters(): array
    {
        return [
            'SQLite' => [static fn (): Adapter => new SQLite(new PDO('sqlite::memory:'))],
            'Memory' => [static fn (): Adapter => new Memory()],
        ];
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testStoredEmptyFormatReadsBackLikeTheDefinition(Closure $adapter): void
    {
        $definition = Attribute::string(key: 'resourceInternalId', size: Database::LENGTH_KEY);

        $database = new Database($adapter(), new Cache(new None()));
        $database
            ->setDatabase('legacy')
            ->setNamespace('legacy_'.\uniqid())
            ->setAuthorization(new Authorization());
        $database->create();
        $database->createCollection(new Collection(id: 'migrations', attributes: [$definition]));

        $legacy = $definition->toDocument()->getArrayCopy();
        $legacy['format'] = '';
        $database->getAuthorization()->skip(fn () => $database->updateDocument(
            Database::METADATA,
            'migrations',
            new Document(['attributes' => [$legacy]]),
        ));

        $stored = $database->getCollection('migrations')->attributes[0];
        $expected = $definition->toDocument();

        $this->assertSame($definition->type, $stored->type);
        foreach (['size', 'required', 'default', 'signed', 'array', 'format', 'formatOptions', 'filters'] as $key) {
            $this->assertSame($expected->getAttribute($key), $stored->getAttribute($key), "Stored '{$key}' differs from the definition");
        }
    }
}
