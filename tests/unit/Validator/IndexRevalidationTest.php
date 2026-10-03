<?php

namespace Tests\Unit\Validator;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Index;

final class IndexRevalidationTest extends TestCase
{
    private const string COLLECTION = 'people';

    public function testUpdatingAnIndexedAttributeDoesNotConflictWithItsOwnIndex(): void
    {
        $database = new Database(new Memory(), new Cache(new None()));
        $database
            ->setDatabase('index_revalidation')
            ->setNamespace('index_revalidation_'.\uniqid());
        $database->getAuthorization()->addRole(Role::any()->toString());
        $database->create();
        $this->assertFalse($database->getAdapter()->supports(Capability::IdenticalIndexes));

        $database->createCollection(new Collection(
            id: self::COLLECTION,
            attributes: [Attribute::string(key: 'name', size: 64)],
            indexes: [Index::key(key: 'by_name', attributes: ['name'])],
        ));

        $updated = $database->updateAttribute(self::COLLECTION, 'name', size: 128);

        $this->assertSame(128, $updated->getAttribute('size'));
        $this->assertSame(128, $database->getCollection(self::COLLECTION)->attributes[0]->size);
    }
}
