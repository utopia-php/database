<?php

namespace Tests\Unit\Documents;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Attribute;
use Utopia\Database\AttributeUpdate;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Format;
use Utopia\Database\Permission;
use Utopia\Database\Role;
use Utopia\Database\Validator\Structure;
use Utopia\Query\Schema\ColumnType;
use Utopia\Validator\Range;
use Utopia\Validator\WhiteList;

final class StoredValueRevalidationTest extends TestCase
{
    private const string COLLECTION = 'rows';

    #[\Override]
    public static function setUpBeforeClass(): void
    {
        Structure::addFormat('storedRange', static function (mixed $attribute): Range {
            /** @var array{formatOptions?: array{min?: int, max?: int}} $attribute */
            return new Range($attribute['formatOptions']['min'] ?? 0, $attribute['formatOptions']['max'] ?? 0, Range::TYPE_INTEGER);
        }, ColumnType::Integer);
        Structure::addFormat('storedEnum', static function (mixed $attribute): WhiteList {
            /** @var array{formatOptions?: array{elements?: list<string>}} $attribute */
            return new WhiteList($attribute['formatOptions']['elements'] ?? [], true);
        }, ColumnType::String);
    }

    public function testAnUpdateFailsOnAStoredValueANarrowedRangeNoLongerAdmits(): void
    {
        $database = $this->database();
        $database->updateAttribute(self::COLLECTION, 'level', new AttributeUpdate(format: new Format('storedRange', ['min' => 0, 'max' => 10])));

        try {
            $database->updateDocument(self::COLLECTION, 'row', new Document(['note' => 'after narrow']));
            $this->fail('An update kept a stored value the narrowed range refuses');
        } catch (StructureException $error) {
            $this->assertSame('Invalid document structure: Attribute "level" has invalid format. Value must be a valid range between 0 and 10', $error->getMessage());
        }

        $this->assertSame('before', $database->getDocument(self::COLLECTION, 'row')->getAttribute('note'));
    }

    public function testAnUpdateFailsOnAStoredValueAShrunkEnumNoLongerAdmits(): void
    {
        $database = $this->database();
        $database->updateAttribute(self::COLLECTION, 'kind', new AttributeUpdate(format: new Format('storedEnum', ['elements' => ['a']])));

        $this->expectException(StructureException::class);
        $this->expectExceptionMessage('Attribute "kind" has invalid format');

        $database->updateDocument(self::COLLECTION, 'row', new Document(['note' => 'after shrink']));
    }

    private function database(): Database
    {
        $database = new Database(new Memory(), new Cache(new None()));
        $database->setDatabase('stored')->setNamespace('stored_'.\uniqid());
        $database->getAuthorization()->addRole(Role::any()->toString());
        $database->create();
        $database->createCollection(Collection::create(
            id: self::COLLECTION,
            attributes: [
                Attribute::integer(key: 'level', format: new Format('storedRange', ['min' => 0, 'max' => 100])),
                Attribute::string(key: 'kind', size: 8, format: new Format('storedEnum', ['elements' => ['a', 'b']])),
                Attribute::string(key: 'note', size: 32),
            ],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any())],
            documentSecurity: false,
        ));
        $database->createDocument(self::COLLECTION, new Document(['$id' => 'row', 'level' => 50, 'kind' => 'b', 'note' => 'before']));

        return $database;
    }
}
