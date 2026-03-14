<?php

namespace Tests\E2E\Adapter\Scopes\Relationships;

use Exception;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Restricted as RestrictedException;
use Utopia\Database\Exception\Structure;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;
use Utopia\Database\Relationship;
use Utopia\Database\RelationType;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\ForeignKeyAction;

trait OneToManyTests
{
    public function testOneToManyOneWayRelationship(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! $database->getAdapter()->supports(Capability::Relationships)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection('artist');
        $database->createCollection('album');

        $database->createAttribute('artist', new Attribute(key: 'name', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute('album', new Attribute(key: 'name', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute('album', new Attribute(key: 'price', type: ColumnType::Double, size: 0, required: true));

        $database->createRelationship(new Relationship(collection: 'artist', relatedCollection: 'album', type: RelationType::OneToMany, key: 'albums'));

        // Check metadata for collection
        $collection = $database->getCollection('artist');
        $attributes = $collection->getAttribute('attributes', []);

        /** @var array<mixed> $attributes */
        foreach ($attributes as $attribute) {
            if ($attribute['key'] === 'albums') {
                $this->assertEquals('relationship', $attribute['type']);
                $this->assertEquals('albums', $attribute['$id']);
                $this->assertEquals('albums', $attribute['key']);
                $this->assertEquals('album', $attribute['options']['relatedCollection']);
                $this->assertEquals(RelationType::OneToMany->value, $attribute['options']['relationType']);
                $this->assertEquals(false, $attribute['options']['twoWay']);
                $this->assertEquals('artist', $attribute['options']['twoWayKey']);
            }
        }

        // Create document with relationship with nested data
        $artist1 = $database->createDocument('artist', new Document([
            '$id' => 'artist1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Artist 1',
            'albums' => [
                [
                    '$id' => 'album1',
                    '$permissions' => [
                        Permission::read(Role::any()),
                        Permission::update(Role::any()),
                    ],
                    'name' => 'Album 1',
                    'price' => 9.99,
                ],
            ],
        ]));

        // Update a document with non existing related document. It should not get added to the list.
        $database->updateDocument('artist', 'artist1', $artist1->setAttribute('albums', ['album1', 'no-album']));

        $artist1Document = $database->getDocument('artist', 'artist1');
        // Assert document does not contain non existing relation document.
        /** @var array<mixed> $_cnt_albums_86 */
        $_cnt_albums_86 = $artist1Document->getAttribute('albums');
        $this->assertEquals(1, \count($_cnt_albums_86));

        // Create document with relationship with related ID
        $database->createDocument('album', new Document([
            '$id' => 'album2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Album 2',
            'price' => 19.99,
        ]));
        $database->createDocument('artist', new Document([
            '$id' => 'artist2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Artist 2',
            'albums' => [
                'album2',
                [
                    '$id' => 'album33',
                    '$permissions' => [
                        Permission::read(Role::any()),
                        Permission::update(Role::any()),
                        Permission::delete(Role::any()),
                    ],
                    'name' => 'Album 3',
                    'price' => 33.33,
                ],
            ],
        ]));

        $documents = $database->find('artist', [
            Query::select(['name']),
            Query::limit(1),
        ]);
        $this->assertArrayNotHasKey('albums', $documents[0]);

        // Get document with relationship
        $artist = $database->getDocument('artist', 'artist1');
        /** @var array<array<string, mixed>> $albums */
        $albums = $artist->getAttribute('albums', []);
        $this->assertEquals('album1', $albums[0]['$id']);
        $this->assertArrayNotHasKey('artist', $albums[0]);

        $artist = $database->getDocument('artist', 'artist2');
        /** @var array<array<string, mixed>> $albums */
        $albums = $artist->getAttribute('albums', []);
        $this->assertEquals('album2', $albums[0]['$id']);
        $this->assertArrayNotHasKey('artist', $albums[0]);
        $this->assertEquals('album33', $albums[1]['$id']);
        $this->assertCount(2, $albums);

        // Get related document
        $album = $database->getDocument('album', 'album1');
        $this->assertArrayNotHasKey('artist', $album);

        $album = $database->getDocument('album', 'album2');
        $this->assertArrayNotHasKey('artist', $album);

        $artists = $database->find('artist');

        $this->assertEquals(2, \count($artists));

        // Select related document attributes
        $artist = $database->findOne('artist', [
            Query::select(['*', 'albums.name']),
        ]);

        if ($artist->isEmpty()) {
            $this->fail('Artist not found');
        }

        /** @var array<Document> $_rel_albums_160 */
        $_rel_albums_160 = $artist->getAttribute('albums');
        $this->assertEquals('Album 1', $_rel_albums_160[0]->getAttribute('name'));
        /** @var array<mixed> $_arr_albums_161 */
        $_arr_albums_161 = $artist->getAttribute('albums');
        $this->assertArrayNotHasKey('price', $_arr_albums_161[0]);

        $artist = $database->getDocument('artist', 'artist1', [
            Query::select(['*', 'albums.name']),
        ]);

        /** @var array<Document> $_rel_albums_167 */
        $_rel_albums_167 = $artist->getAttribute('albums');
        $this->assertEquals('Album 1', $_rel_albums_167[0]->getAttribute('name'));
        /** @var array<mixed> $_arr_albums_168 */
        $_arr_albums_168 = $artist->getAttribute('albums');
        $this->assertArrayNotHasKey('price', $_arr_albums_168[0]);

        // Update root document attribute without altering relationship
        $artist1 = $database->updateDocument(
            'artist',
            $artist1->getId(),
            $artist1->setAttribute('name', 'Artist 1 Updated')
        );

        $this->assertEquals('Artist 1 Updated', $artist1->getAttribute('name'));
        $artist1 = $database->getDocument('artist', 'artist1');
        $this->assertEquals('Artist 1 Updated', $artist1->getAttribute('name'));

        // Update nested document attribute
        /** @var array<\Utopia\Database\Document> $albums */
        $albums = $artist1->getAttribute('albums', []);
        $albums[0]->setAttribute('name', 'Album 1 Updated');

        $artist1 = $database->updateDocument(
            'artist',
            $artist1->getId(),
            $artist1->setAttribute('albums', $albums)
        );

        /** @var array<Document> $_rel_albums_191 */
        $_rel_albums_191 = $artist1->getAttribute('albums');
        $this->assertEquals('Album 1 Updated', $_rel_albums_191[0]->getAttribute('name'));
        $artist1 = $database->getDocument('artist', 'artist1');
        /** @var array<Document> $_rel_albums_193 */
        $_rel_albums_193 = $artist1->getAttribute('albums');
        $this->assertEquals('Album 1 Updated', $_rel_albums_193[0]->getAttribute('name'));

        $albumId = $artist1->getAttribute('albums')[0]->getAttribute('$id');
        $albumDocument = $database->getDocument('album', $albumId);
        $albumDocument->setAttribute('name', 'Album 1 Updated!!!');
        $database->updateDocument('album', $albumDocument->getId(), $albumDocument);
        $albumDocument = $database->getDocument('album', $albumDocument->getId());
        $artist1 = $database->getDocument('artist', $artist1->getId());

        $this->assertEquals('Album 1 Updated!!!', $albumDocument['name']);
        /** @var array<Document> $_arr_albums_203 */
        $_arr_albums_203 = $artist1->getAttribute('albums');
        $this->assertEquals($albumDocument->getId(), $_arr_albums_203[0]->getId());
        $this->assertEquals($albumDocument->getAttribute('name'), $artist1->getAttribute('albums')[0]->getAttribute('name'));

        // Create new document with no relationship
        $artist3 = $database->createDocument('artist', new Document([
            '$id' => 'artist3',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Artist 3',
        ]));

        // Update to relate to created document
        $artist3 = $database->updateDocument(
            'artist',
            $artist3->getId(),
            $artist3->setAttribute('albums', [new Document([
                '$id' => 'album3',
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'name' => 'Album 3',
                'price' => 29.99,
            ])])
        );

        /** @var array<Document> $_rel_albums_233 */
        $_rel_albums_233 = $artist3->getAttribute('albums');
        $this->assertEquals('Album 3', $_rel_albums_233[0]->getAttribute('name'));
        $artist3 = $database->getDocument('artist', 'artist3');
        /** @var array<Document> $_rel_albums_235 */
        $_rel_albums_235 = $artist3->getAttribute('albums');
        $this->assertEquals('Album 3', $_rel_albums_235[0]->getAttribute('name'));

        // Update document with new related documents, will remove existing relations
        $database->updateDocument(
            'artist',
            $artist1->getId(),
            $artist1->setAttribute('albums', ['album2'])
        );

        // Update document with new related documents, will remove existing relations
        $database->updateDocument(
            'artist',
            $artist1->getId(),
            $artist1->setAttribute('albums', ['album1', 'album2'])
        );

        // Rename relationship key
        $database->updateRelationship(
            'artist',
            'albums',
            'newAlbums'
        );

        // Get document with new relationship key
        $artist = $database->getDocument('artist', 'artist1');
        /** @var array<array<string, mixed>> $albums */
        $albums = $artist->getAttribute('newAlbums');
        $this->assertEquals('album1', $albums[0]['$id']);

        // Create new document with no relationship
        $database->createDocument('artist', new Document([
            '$id' => 'artist4',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Artist 4',
        ]));

        // Can delete document with no relationship when on delete is set to restrict
        $deleted = $database->deleteDocument('artist', 'artist4');
        $this->assertEquals(true, $deleted);

        $artist4 = $database->getDocument('artist', 'artist4');
        $this->assertEquals(true, $artist4->isEmpty());

        // Try to delete document while still related to another with on delete: restrict
        try {
            $database->deleteDocument('artist', 'artist1');
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Cannot delete document because it has at least one related document.', $e->getMessage());
        }

        // Change on delete to set null
        $database->updateRelationship(
            collection: 'artist',
            id: 'newAlbums',
            onDelete: ForeignKeyAction::SetNull
        );

        // Delete parent, set child relationship to null
        $database->deleteDocument('artist', 'artist1');

        // Check relation was set to null
        $album2 = $database->getDocument('album', 'album2');
        $this->assertEquals(null, $album2->getAttribute('artist', ''));

        // Relate again
        $database->updateDocument(
            'album',
            $album2->getId(),
            $album2->setAttribute('artist', 'artist2')
        );

        // Change on delete to cascade
        $database->updateRelationship(
            collection: 'artist',
            id: 'newAlbums',
            onDelete: ForeignKeyAction::Cascade
        );

        // Delete parent, will delete child
        $database->deleteDocument('artist', 'artist2');

        // Check parent and child were deleted
        $library = $database->getDocument('artist', 'artist2');
        $this->assertEquals(true, $library->isEmpty());

        $library = $database->getDocument('album', 'album2');
        $this->assertEquals(true, $library->isEmpty());

        $albums = [];
        for ($i = 1; $i <= 50; $i++) {
            $albums[] = [
                '$id' => 'album_'.$i,
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'name' => 'album '.$i.' '.'Artist 100',
                'price' => 100,
            ];
        }

        $artist = $database->createDocument('artist', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Artist 100',
            'newAlbums' => $albums,
        ]));

        $artist = $database->getDocument('artist', $artist->getId());
        /** @var array<mixed> $_ac_newAlbums_351 */
        $_ac_newAlbums_351 = $artist->getAttribute('newAlbums');
        $this->assertCount(50, $_ac_newAlbums_351);

        $albums = $database->find('album', [
            Query::equal('artist', [$artist->getId()]),
            Query::limit(999),
        ]);

        $this->assertCount(50, $albums);

        $count = $database->count('album', [
            Query::equal('artist', [$artist->getId()]),
        ]);

        $this->assertEquals(50, $count);

        $database->deleteDocument('album', 'album_1');
        $artist = $database->getDocument('artist', $artist->getId());
        /** @var array<mixed> $_ac_newAlbums_368 */
        $_ac_newAlbums_368 = $artist->getAttribute('newAlbums');
        $this->assertCount(49, $_ac_newAlbums_368);

        $database->deleteDocument('artist', $artist->getId());

        $albums = $database->find('album', [
            Query::equal('artist', [$artist->getId()]),
            Query::limit(999),
        ]);

        $this->assertCount(0, $albums);

        // Delete relationship
        $database->deleteRelationship(
            'artist',
            'newAlbums'
        );

        // Try to get document again
        $artist = $database->getDocument('artist', 'artist1');
        $albums = $artist->getAttribute('newAlbums', '');
        $this->assertEquals(null, $albums);
    }

    public function testOneToManyTwoWayRelationship(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! $database->getAdapter()->supports(Capability::Relationships)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection('customer');
        $database->createCollection('account');

        $database->createAttribute('customer', new Attribute(key: 'name', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute('account', new Attribute(key: 'name', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute('account', new Attribute(key: 'number', type: ColumnType::String, size: 255, required: true));

        $database->createRelationship(new Relationship(collection: 'customer', relatedCollection: 'account', type: RelationType::OneToMany, twoWay: true, key: 'accounts'));

        // Check metadata for collection
        $collection = $database->getCollection('customer');
        $attributes = $collection->getAttribute('attributes', []);
        /** @var array<mixed> $attributes */
        foreach ($attributes as $attribute) {
            if ($attribute['key'] === 'accounts') {
                $this->assertEquals('relationship', $attribute['type']);
                $this->assertEquals('accounts', $attribute['$id']);
                $this->assertEquals('accounts', $attribute['key']);
                $this->assertEquals('account', $attribute['options']['relatedCollection']);
                $this->assertEquals(RelationType::OneToMany->value, $attribute['options']['relationType']);
                $this->assertEquals(true, $attribute['options']['twoWay']);
                $this->assertEquals('customer', $attribute['options']['twoWayKey']);
            }
        }

        // Check metadata for related collection
        $collection = $database->getCollection('account');
        $attributes = $collection->getAttribute('attributes', []);
        /** @var array<mixed> $attributes */
        foreach ($attributes as $attribute) {
            if ($attribute['key'] === 'customer') {
                $this->assertEquals('relationship', $attribute['type']);
                $this->assertEquals('customer', $attribute['$id']);
                $this->assertEquals('customer', $attribute['key']);
                $this->assertEquals('customer', $attribute['options']['relatedCollection']);
                $this->assertEquals(RelationType::OneToMany->value, $attribute['options']['relationType']);
                $this->assertEquals(true, $attribute['options']['twoWay']);
                $this->assertEquals('accounts', $attribute['options']['twoWayKey']);
            }
        }

        // Create document with relationship with nested data
        $customer1 = $database->createDocument('customer', new Document([
            '$id' => 'customer1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Customer 1',
            'accounts' => [
                [
                    '$id' => 'account1',
                    '$permissions' => [
                        Permission::read(Role::any()),
                        Permission::update(Role::any()),
                        Permission::delete(Role::any()),
                    ],
                    'name' => 'Account 1',
                    'number' => '123456789',
                ],
            ],
        ]));

        // Update a document with non existing related document. It should not get added to the list.
        $database->updateDocument('customer', 'customer1', $customer1->setAttribute('accounts', ['account1', 'no-account']));

        $customer1Document = $database->getDocument('customer', 'customer1');
        // Assert document does not contain non existing relation document.
        /** @var array<mixed> $_cnt_accounts_469 */
        $_cnt_accounts_469 = $customer1Document->getAttribute('accounts');
        $this->assertEquals(1, \count($_cnt_accounts_469));

        // Create document with relationship with related ID
        $account2 = $database->createDocument('account', new Document([
            '$id' => 'account2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Account 2',
            'number' => '987654321',
        ]));
        $database->createDocument('customer', new Document([
            '$id' => 'customer2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Customer 2',
            'accounts' => [
                'account2',
            ],
        ]));

        // Create from child side
        $database->createDocument('account', new Document([
            '$id' => 'account3',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Account 3',
            'number' => '123456789',
            'customer' => [
                '$id' => 'customer3',
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'name' => 'Customer 3',
            ],
        ]));
        $database->createDocument('customer', new Document([
            '$id' => 'customer4',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Customer 4',
        ]));
        $database->createDocument('account', new Document([
            '$id' => 'account4',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Account 4',
            'number' => '123456789',
            'customer' => 'customer4',
        ]));

        // Get documents with relationship
        $customer = $database->getDocument('customer', 'customer1');
        /** @var array<array<string, mixed>> $accounts */
        $accounts = $customer->getAttribute('accounts', []);
        $this->assertEquals('account1', $accounts[0]['$id']);
        $this->assertArrayNotHasKey('customer', $accounts[0]);

        $customer = $database->getDocument('customer', 'customer2');
        /** @var array<array<string, mixed>> $accounts */
        $accounts = $customer->getAttribute('accounts', []);
        $this->assertEquals('account2', $accounts[0]['$id']);
        $this->assertArrayNotHasKey('customer', $accounts[0]);

        $customer = $database->getDocument('customer', 'customer3');
        /** @var array<array<string, mixed>> $accounts */
        $accounts = $customer->getAttribute('accounts', []);
        $this->assertEquals('account3', $accounts[0]['$id']);
        $this->assertArrayNotHasKey('customer', $accounts[0]);

        $customer = $database->getDocument('customer', 'customer4');
        /** @var array<array<string, mixed>> $accounts */
        $accounts = $customer->getAttribute('accounts', []);
        $this->assertEquals('account4', $accounts[0]['$id']);
        $this->assertArrayNotHasKey('customer', $accounts[0]);

        // Get related documents
        $account = $database->getDocument('account', 'account1');
        $customer = $account->getAttribute('customer');
        $this->assertEquals('customer1', $customer['$id']);
        $this->assertArrayNotHasKey('accounts', $customer);

        $account = $database->getDocument('account', 'account2');
        $customer = $account->getAttribute('customer');
        $this->assertEquals('customer2', $customer['$id']);
        $this->assertArrayNotHasKey('accounts', $customer);

        $account = $database->getDocument('account', 'account3');
        $customer = $account->getAttribute('customer');
        $this->assertEquals('customer3', $customer['$id']);
        $this->assertArrayNotHasKey('accounts', $customer);

        $account = $database->getDocument('account', 'account4');
        $customer = $account->getAttribute('customer');
        $this->assertEquals('customer4', $customer['$id']);
        $this->assertArrayNotHasKey('accounts', $customer);

        $customers = $database->find('customer');

        $this->assertEquals(4, \count($customers));

        // Select related document attributes
        $customer = $database->findOne('customer', [
            Query::select(['*', 'accounts.name']),
        ]);

        if ($customer->isEmpty()) {
            throw new Exception('Customer not found');
        }

        /** @var array<Document> $_rel_accounts_591 */
        $_rel_accounts_591 = $customer->getAttribute('accounts');
        $this->assertEquals('Account 1', $_rel_accounts_591[0]->getAttribute('name'));
        /** @var array<mixed> $_arr_accounts_592 */
        $_arr_accounts_592 = $customer->getAttribute('accounts');
        $this->assertArrayNotHasKey('number', $_arr_accounts_592[0]);

        $customer = $database->getDocument('customer', 'customer1', [
            Query::select(['*', 'accounts.name']),
        ]);

        /** @var array<Document> $_rel_accounts_598 */
        $_rel_accounts_598 = $customer->getAttribute('accounts');
        $this->assertEquals('Account 1', $_rel_accounts_598[0]->getAttribute('name'));
        /** @var array<mixed> $_arr_accounts_599 */
        $_arr_accounts_599 = $customer->getAttribute('accounts');
        $this->assertArrayNotHasKey('number', $_arr_accounts_599[0]);

        // Update root document attribute without altering relationship
        $customer1 = $database->updateDocument(
            'customer',
            $customer1->getId(),
            $customer1->setAttribute('name', 'Customer 1 Updated')
        );

        $this->assertEquals('Customer 1 Updated', $customer1->getAttribute('name'));
        $customer1 = $database->getDocument('customer', 'customer1');
        $this->assertEquals('Customer 1 Updated', $customer1->getAttribute('name'));

        $account2 = $database->getDocument('account', 'account2');

        // Update inverse root document attribute without altering relationship
        $account2 = $database->updateDocument(
            'account',
            $account2->getId(),
            $account2->setAttribute('name', 'Account 2 Updated')
        );

        $this->assertEquals('Account 2 Updated', $account2->getAttribute('name'));
        $account2 = $database->getDocument('account', 'account2');
        $this->assertEquals('Account 2 Updated', $account2->getAttribute('name'));

        // Update nested document attribute
        /** @var array<\Utopia\Database\Document> $accounts */
        $accounts = $customer1->getAttribute('accounts', []);
        $accounts[0]->setAttribute('name', 'Account 1 Updated');

        $customer1 = $database->updateDocument(
            'customer',
            $customer1->getId(),
            $customer1->setAttribute('accounts', $accounts)
        );

        /** @var array<Document> $_rel_accounts_635 */
        $_rel_accounts_635 = $customer1->getAttribute('accounts');
        $this->assertEquals('Account 1 Updated', $_rel_accounts_635[0]->getAttribute('name'));
        $customer1 = $database->getDocument('customer', 'customer1');
        /** @var array<Document> $_rel_accounts_637 */
        $_rel_accounts_637 = $customer1->getAttribute('accounts');
        $this->assertEquals('Account 1 Updated', $_rel_accounts_637[0]->getAttribute('name'));

        // Update inverse nested document attribute
        $account2 = $database->updateDocument(
            'account',
            $account2->getId(),
            $account2->setAttribute(
                'customer',
                $account2
                    ->getAttribute('customer')
                    ->setAttribute('name', 'Customer 2 Updated')
            )
        );

        /** @var \Utopia\Database\Document $_doc_customer_651 */
        $_doc_customer_651 = $account2->getAttribute('customer');
        $this->assertEquals('Customer 2 Updated', $_doc_customer_651->getAttribute('name'));
        $account2 = $database->getDocument('account', 'account2');
        /** @var \Utopia\Database\Document $_doc_customer_653 */
        $_doc_customer_653 = $account2->getAttribute('customer');
        $this->assertEquals('Customer 2 Updated', $_doc_customer_653->getAttribute('name'));

        // Create new document with no relationship
        $customer5 = $database->createDocument('customer', new Document([
            '$id' => 'customer5',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Customer 5',
        ]));

        // Update to relate to created document
        $customer5 = $database->updateDocument(
            'customer',
            $customer5->getId(),
            $customer5->setAttribute('accounts', [new Document([
                '$id' => 'account5',
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'name' => 'Account 5',
                'number' => '123456789',
            ])])
        );

        /** @var array<Document> $_rel_accounts_682 */
        $_rel_accounts_682 = $customer5->getAttribute('accounts');
        $this->assertEquals('Account 5', $_rel_accounts_682[0]->getAttribute('name'));
        $customer5 = $database->getDocument('customer', 'customer5');
        /** @var array<Document> $_rel_accounts_684 */
        $_rel_accounts_684 = $customer5->getAttribute('accounts');
        $this->assertEquals('Account 5', $_rel_accounts_684[0]->getAttribute('name'));

        // Create new child document with no relationship
        $account6 = $database->createDocument('account', new Document([
            '$id' => 'account6',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Account 6',
            'number' => '123456789',
        ]));

        // Update inverse to relate to created document
        $account6 = $database->updateDocument(
            'account',
            $account6->getId(),
            $account6->setAttribute('customer', new Document([
                '$id' => 'customer6',
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'name' => 'Customer 6',
            ]))
        );

        /** @var \Utopia\Database\Document $_doc_customer_713 */
        $_doc_customer_713 = $account6->getAttribute('customer');
        $this->assertEquals('Customer 6', $_doc_customer_713->getAttribute('name'));
        $account6 = $database->getDocument('account', 'account6');
        /** @var \Utopia\Database\Document $_doc_customer_715 */
        $_doc_customer_715 = $account6->getAttribute('customer');
        $this->assertEquals('Customer 6', $_doc_customer_715->getAttribute('name'));

        // Update document with new related document, will remove existing relations
        $database->updateDocument(
            'customer',
            $customer1->getId(),
            $customer1->setAttribute('accounts', ['account2'])
        );

        // Update document with new related document
        $database->updateDocument(
            'customer',
            $customer1->getId(),
            $customer1->setAttribute('accounts', ['account1', 'account2'])
        );

        // Update inverse document
        $database->updateDocument(
            'account',
            $account2->getId(),
            $account2->setAttribute('customer', 'customer2')
        );

        // Rename relationship keys on both sides
        $database->updateRelationship(
            'customer',
            'accounts',
            'newAccounts',
            'newCustomer'
        );

        // Get document with new relationship key
        $customer = $database->getDocument('customer', 'customer1');
        /** @var array<array<string, mixed>> $accounts */
        $accounts = $customer->getAttribute('newAccounts');
        $this->assertEquals('account1', $accounts[0]['$id']);

        // Get inverse document with new relationship key
        $account = $database->getDocument('account', 'account1');
        $customer = $account->getAttribute('newCustomer');
        $this->assertEquals('customer1', $customer['$id']);

        // Create new document with no relationship
        $database->createDocument('customer', new Document([
            '$id' => 'customer7',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Customer 7',
        ]));

        // Can delete document with no relationship when on delete is set to restrict
        $deleted = $database->deleteDocument('customer', 'customer7');
        $this->assertEquals(true, $deleted);

        $customer7 = $database->getDocument('customer', 'customer7');
        $this->assertEquals(true, $customer7->isEmpty());

        // Try to delete document while still related to another with on delete: restrict
        try {
            $database->deleteDocument('customer', 'customer1');
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Cannot delete document because it has at least one related document.', $e->getMessage());
        }

        // Change on delete to set null
        $database->updateRelationship(
            collection: 'customer',
            id: 'newAccounts',
            onDelete: ForeignKeyAction::SetNull
        );

        // Delete parent, set child relationship to null
        $database->deleteDocument('customer', 'customer1');

        // Check relation was set to null
        $account1 = $database->getDocument('account', 'account1');
        $this->assertEquals(null, $account2->getAttribute('newCustomer', ''));

        // Relate again
        $database->updateDocument(
            'account',
            $account1->getId(),
            $account1->setAttribute('newCustomer', 'customer2')
        );

        // Change on delete to cascade
        $database->updateRelationship(
            collection: 'customer',
            id: 'newAccounts',
            onDelete: ForeignKeyAction::Cascade
        );

        // Delete parent, will delete child
        $database->deleteDocument('customer', 'customer2');

        // Check parent and child were deleted
        $library = $database->getDocument('customer', 'customer2');
        $this->assertEquals(true, $library->isEmpty());

        $library = $database->getDocument('account', 'account2');
        $this->assertEquals(true, $library->isEmpty());

        // Delete relationship
        $database->deleteRelationship(
            'customer',
            'newAccounts'
        );

        // Try to get document again
        $customer = $database->getDocument('customer', 'customer1');
        $accounts = $customer->getAttribute('newAccounts');
        $this->assertEquals(null, $accounts);

        // Try to get inverse document again
        $accounts = $database->getDocument('account', 'account1');
        $customer = $accounts->getAttribute('newCustomer');
        $this->assertEquals(null, $customer);
    }

    public function testNestedOneToMany_OneToOneRelationship(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! $database->getAdapter()->supports(Capability::Relationships)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection('countries');
        $database->createCollection('cities');
        $database->createCollection('mayors');

        $database->createAttribute('cities', new Attribute(key: 'name', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute('countries', new Attribute(key: 'name', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute('mayors', new Attribute(key: 'name', type: ColumnType::String, size: 255, required: true));

        $database->createRelationship(new Relationship(collection: 'countries', relatedCollection: 'cities', type: RelationType::OneToMany, twoWay: true, twoWayKey: 'country'));
        $database->createRelationship(new Relationship(collection: 'cities', relatedCollection: 'mayors', type: RelationType::OneToOne, twoWay: true, key: 'mayor', twoWayKey: 'city'));

        $database->createDocument('countries', new Document([
            '$id' => 'country1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'name' => 'Country 1',
            'cities' => [
                [
                    '$id' => 'city1',
                    '$permissions' => [
                        Permission::read(Role::any()),
                        Permission::update(Role::any()),
                    ],
                    'name' => 'City 1',
                    'mayor' => [
                        '$id' => 'mayor1',
                        '$permissions' => [
                            Permission::read(Role::any()),
                            Permission::update(Role::any()),
                        ],
                        'name' => 'Mayor 1',
                    ],
                ],
                [
                    '$id' => 'city2',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'City 2',
                    'mayor' => [
                        '$id' => 'mayor2',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'name' => 'Mayor 2',
                    ],
                ],
            ],
        ]));

        $documents = $database->find('countries', [
            Query::limit(1),
        ]);
        $this->assertEquals('Mayor 1', $documents[0]['cities'][0]['mayor']['name']);

        $documents = $database->find('countries', [
            Query::select(['name']),
            Query::limit(1),
        ]);
        $this->assertArrayHasKey('name', $documents[0]);
        $this->assertArrayNotHasKey('cities', $documents[0]);

        $documents = $database->find('countries', [
            Query::select(['*']),
            Query::limit(1),
        ]);
        $this->assertArrayHasKey('name', $documents[0]);
        $this->assertArrayNotHasKey('cities', $documents[0]);

        $documents = $database->find('countries', [
            Query::select(['*', 'cities.*', 'cities.mayor.*']),
            Query::limit(1),
        ]);

        $this->assertEquals('Mayor 1', $documents[0]['cities'][0]['mayor']['name']);

        // Insert docs to cache:
        $country1 = $database->getDocument('countries', 'country1');
        $mayor1 = $database->getDocument('mayors', 'mayor1');
        $this->assertEquals('City 1', $mayor1['city']['name']);
        $this->assertEquals('City 1', $country1['cities'][0]['name']);

        $database->updateDocument('cities', 'city1', new Document([
            '$id' => 'city1',
            '$collection' => 'cities',
            'name' => 'City 1 updated',
            'mayor' => 'mayor1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
        ]));

        $mayor1 = $database->getDocument('mayors', 'mayor1');
        $country1 = $database->getDocument('countries', 'country1');

        $this->assertEquals('City 1 updated', $mayor1['city']['name']);
        $this->assertEquals('City 1 updated', $country1['cities'][0]['name']);
        $this->assertEquals('city1', $country1['cities'][0]['$id']);
        $this->assertEquals('city2', $country1['cities'][1]['$id']);
        $this->assertEquals('mayor1', $country1['cities'][0]['mayor']['$id']);
        $this->assertEquals('mayor2', $country1['cities'][1]['mayor']['$id']);
        $this->assertArrayNotHasKey('city', $country1['cities'][0]['mayor']);
        $this->assertArrayNotHasKey('city', $country1['cities'][1]['mayor']);

        $database->createDocument('mayors', new Document([
            '$id' => 'mayor3',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Mayor 3',
            'city' => [
                '$id' => 'city3',
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
                'name' => 'City 3',
                'country' => [
                    '$id' => 'country2',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Country 2',
                ],
            ],
        ]));

        $country2 = $database->getDocument('countries', 'country2');
        $this->assertEquals('city3', $country2['cities'][0]['$id']);
        $this->assertEquals('mayor3', $country2['cities'][0]['mayor']['$id']);
        $this->assertArrayNotHasKey('country', $country2['cities'][0]);
        $this->assertArrayNotHasKey('city', $country2['cities'][0]['mayor']);
    }

    public function testNestedOneToMany_OneToManyRelationship(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! $database->getAdapter()->supports(Capability::Relationships)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection('dormitories');
        $database->createCollection('occupants');
        $database->createCollection('pets');

        $database->createAttribute('dormitories', new Attribute(key: 'name', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute('occupants', new Attribute(key: 'name', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute('pets', new Attribute(key: 'name', type: ColumnType::String, size: 255, required: true));

        $database->createRelationship(new Relationship(collection: 'dormitories', relatedCollection: 'occupants', type: RelationType::OneToMany, twoWay: true, twoWayKey: 'dormitory'));
        $database->createRelationship(new Relationship(collection: 'occupants', relatedCollection: 'pets', type: RelationType::OneToMany, twoWay: true, twoWayKey: 'occupant'));

        $database->createDocument('dormitories', new Document([
            '$id' => 'dormitory1',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'House 1',
            'occupants' => [
                [
                    '$id' => 'occupant1',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Occupant 1',
                    'pets' => [
                        [
                            '$id' => 'pet1',
                            '$permissions' => [
                                Permission::read(Role::any()),
                            ],
                            'name' => 'Pet 1',
                        ],
                        [
                            '$id' => 'pet2',
                            '$permissions' => [
                                Permission::read(Role::any()),
                            ],
                            'name' => 'Pet 2',
                        ],
                    ],
                ],
                [
                    '$id' => 'occupant2',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Occupant 2',
                    'pets' => [
                        [
                            '$id' => 'pet3',
                            '$permissions' => [
                                Permission::read(Role::any()),
                            ],
                            'name' => 'Pet 3',
                        ],
                        [
                            '$id' => 'pet4',
                            '$permissions' => [
                                Permission::read(Role::any()),
                            ],
                            'name' => 'Pet 4',
                        ],
                    ],
                ],
            ],
        ]));

        $dormitory1 = $database->getDocument('dormitories', 'dormitory1');
        $this->assertEquals('occupant1', $dormitory1['occupants'][0]['$id']);
        $this->assertEquals('occupant2', $dormitory1['occupants'][1]['$id']);
        $this->assertEquals('pet1', $dormitory1['occupants'][0]['pets'][0]['$id']);
        $this->assertEquals('pet2', $dormitory1['occupants'][0]['pets'][1]['$id']);
        $this->assertEquals('pet3', $dormitory1['occupants'][1]['pets'][0]['$id']);
        $this->assertEquals('pet4', $dormitory1['occupants'][1]['pets'][1]['$id']);
        $this->assertArrayNotHasKey('dormitory', $dormitory1['occupants'][0]);
        $this->assertArrayNotHasKey('dormitory', $dormitory1['occupants'][1]);
        $this->assertArrayNotHasKey('occupant', $dormitory1['occupants'][0]['pets'][0]);
        $this->assertArrayNotHasKey('occupant', $dormitory1['occupants'][0]['pets'][1]);
        $this->assertArrayNotHasKey('occupant', $dormitory1['occupants'][1]['pets'][0]);
        $this->assertArrayNotHasKey('occupant', $dormitory1['occupants'][1]['pets'][1]);

        $database->createDocument('pets', new Document([
            '$id' => 'pet5',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Pet 5',
            'occupant' => [
                '$id' => 'occupant3',
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
                'name' => 'Occupant 3',
                'dormitory' => [
                    '$id' => 'dormitory2',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'House 2',
                ],
            ],
        ]));

        $pet5 = $database->getDocument('pets', 'pet5');
        $this->assertEquals('occupant3', $pet5['occupant']['$id']);
        $this->assertEquals('dormitory2', $pet5['occupant']['dormitory']['$id']);
        $this->assertArrayNotHasKey('pets', $pet5['occupant']);
        $this->assertArrayNotHasKey('occupant', $pet5['occupant']['dormitory']);
    }

    public function testNestedOneToMany_ManyToOneRelationship(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! $database->getAdapter()->supports(Capability::Relationships)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection('home');
        $database->createCollection('renters');
        $database->createCollection('floors');

        $database->createAttribute('home', new Attribute(key: 'name', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute('renters', new Attribute(key: 'name', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute('floors', new Attribute(key: 'name', type: ColumnType::String, size: 255, required: true));

        $database->createRelationship(new Relationship(collection: 'home', relatedCollection: 'renters', type: RelationType::OneToMany, twoWay: true));
        $database->createRelationship(new Relationship(collection: 'renters', relatedCollection: 'floors', type: RelationType::ManyToOne, twoWay: true, key: 'floor'));

        $database->createDocument('home', new Document([
            '$id' => 'home1',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'House 1',
            'renters' => [
                [
                    '$id' => 'renter1',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Occupant 1',
                    'floor' => [
                        '$id' => 'floor1',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'name' => 'Floor 1',
                    ],
                ],
            ],
        ]));

        $home1 = $database->getDocument('home', 'home1');
        $this->assertEquals('renter1', $home1['renters'][0]['$id']);
        $this->assertEquals('floor1', $home1['renters'][0]['floor']['$id']);
        $this->assertArrayNotHasKey('home', $home1['renters'][0]);
        $this->assertArrayNotHasKey('renters', $home1['renters'][0]['floor']);

        $database->createDocument('floors', new Document([
            '$id' => 'floor2',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Floor 2',
            'renters' => [
                [
                    '$id' => 'renter2',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Occupant 2',
                    'home' => [
                        '$id' => 'home2',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'name' => 'House 2',
                    ],
                ],
            ],
        ]));

        $floor2 = $database->getDocument('floors', 'floor2');
        $this->assertEquals('renter2', $floor2['renters'][0]['$id']);
        $this->assertArrayNotHasKey('floor', $floor2['renters'][0]);
        $this->assertEquals('home2', $floor2['renters'][0]['home']['$id']);
        $this->assertArrayNotHasKey('renter', $floor2['renters'][0]['home']);
    }

    public function testNestedOneToMany_ManyToManyRelationship(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! $database->getAdapter()->supports(Capability::Relationships)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection('owners');
        $database->createCollection('cats');
        $database->createCollection('toys');

        $database->createAttribute('owners', new Attribute(key: 'name', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute('cats', new Attribute(key: 'name', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute('toys', new Attribute(key: 'name', type: ColumnType::String, size: 255, required: true));

        $database->createRelationship(new Relationship(collection: 'owners', relatedCollection: 'cats', type: RelationType::OneToMany, twoWay: true, twoWayKey: 'owner'));
        $database->createRelationship(new Relationship(collection: 'cats', relatedCollection: 'toys', type: RelationType::ManyToMany, twoWay: true));

        $database->createDocument('owners', new Document([
            '$id' => 'owner1',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Owner 1',
            'cats' => [
                [
                    '$id' => 'cat1',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Pet 1',
                    'toys' => [
                        [
                            '$id' => 'toy1',
                            '$permissions' => [
                                Permission::read(Role::any()),
                            ],
                            'name' => 'Toy 1',
                        ],
                    ],
                ],
            ],
        ]));

        $owner1 = $database->getDocument('owners', 'owner1');
        $this->assertEquals('cat1', $owner1['cats'][0]['$id']);
        $this->assertArrayNotHasKey('owner', $owner1['cats'][0]);
        $this->assertEquals('toy1', $owner1['cats'][0]['toys'][0]['$id']);
        $this->assertArrayNotHasKey('cats', $owner1['cats'][0]['toys'][0]);

        $database->createDocument('toys', new Document([
            '$id' => 'toy2',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Toy 2',
            'cats' => [
                [
                    '$id' => 'cat2',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Pet 2',
                    'owner' => [
                        '$id' => 'owner2',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'name' => 'Owner 2',
                    ],
                ],
            ],
        ]));

        $toy2 = $database->getDocument('toys', 'toy2');
        $this->assertEquals('cat2', $toy2['cats'][0]['$id']);
        $this->assertArrayNotHasKey('toys', $toy2['cats'][0]);
        $this->assertEquals('owner2', $toy2['cats'][0]['owner']['$id']);
        $this->assertArrayNotHasKey('cats', $toy2['cats'][0]['owner']);
    }

    public function testExceedMaxDepthOneToMany(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! $database->getAdapter()->supports(Capability::Relationships)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $level1Collection = 'level1OneToMany';
        $level2Collection = 'level2OneToMany';
        $level3Collection = 'level3OneToMany';
        $level4Collection = 'level4OneToMany';

        $database->createCollection($level1Collection);
        $database->createCollection($level2Collection);
        $database->createCollection($level3Collection);
        $database->createCollection($level4Collection);

        $database->createRelationship(new Relationship(collection: $level1Collection, relatedCollection: $level2Collection, type: RelationType::OneToMany, twoWay: true));
        $database->createRelationship(new Relationship(collection: $level2Collection, relatedCollection: $level3Collection, type: RelationType::OneToMany, twoWay: true));
        $database->createRelationship(new Relationship(collection: $level3Collection, relatedCollection: $level4Collection, type: RelationType::OneToMany, twoWay: true));

        // Exceed create depth
        $level1 = $database->createDocument($level1Collection, new Document([
            '$id' => 'level1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            $level2Collection => [
                [
                    '$id' => 'level2',
                    $level3Collection => [
                        [
                            '$id' => 'level3',
                            $level4Collection => [
                                [
                                    '$id' => 'level4',
                                ],
                            ],
                        ],
                    ],
                ],
            ],
        ]));
        $this->assertEquals(1, count($level1[$level2Collection]));
        $this->assertEquals('level2', $level1[$level2Collection][0]->getId());
        $this->assertEquals(1, count($level1[$level2Collection][0][$level3Collection]));
        $this->assertEquals('level3', $level1[$level2Collection][0][$level3Collection][0]->getId());
        $this->assertArrayNotHasKey('level4', $level1[$level2Collection][0][$level3Collection][0]);

        // Make sure level 4 document was not created
        $level3 = $database->getDocument($level3Collection, 'level3');
        $this->assertEquals(0, count($level3[$level4Collection]));
        $level4 = $database->getDocument($level4Collection, 'level4');
        $this->assertTrue($level4->isEmpty());

        // Exceed fetch depth
        $level1 = $database->getDocument($level1Collection, 'level1');
        $this->assertEquals(1, count($level1[$level2Collection]));
        $this->assertEquals('level2', $level1[$level2Collection][0]->getId());
        $this->assertEquals(1, count($level1[$level2Collection][0][$level3Collection]));
        $this->assertEquals('level3', $level1[$level2Collection][0][$level3Collection][0]->getId());
        $this->assertArrayNotHasKey($level4Collection, $level1[$level2Collection][0][$level3Collection][0]);

        // Exceed update depth
        $level1 = $database->updateDocument(
            $level1Collection,
            'level1',
            $level1
                ->setAttribute($level2Collection, [new Document([
                    '$id' => 'level2new',
                    $level3Collection => [
                        [
                            '$id' => 'level3new',
                            $level4Collection => [
                                [
                                    '$id' => 'level4new',
                                ],
                            ],
                        ],
                    ],
                ])])
        );
        $this->assertEquals(1, count($level1[$level2Collection]));
        $this->assertEquals('level2new', $level1[$level2Collection][0]->getId());
        $this->assertEquals(1, count($level1[$level2Collection][0][$level3Collection]));
        $this->assertEquals('level3new', $level1[$level2Collection][0][$level3Collection][0]->getId());
        $this->assertArrayNotHasKey($level4Collection, $level1[$level2Collection][0][$level3Collection][0]);

        // Make sure level 4 document was not created
        $level3 = $database->getDocument($level3Collection, 'level3new');
        $this->assertEquals(0, count($level3[$level4Collection]));
        $level4 = $database->getDocument($level4Collection, 'level4new');
        $this->assertTrue($level4->isEmpty());
    }

    public function testExceedMaxDepthOneToManyChild(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! $database->getAdapter()->supports(Capability::Relationships)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $level1Collection = 'level1OneToManyChild';
        $level2Collection = 'level2OneToManyChild';
        $level3Collection = 'level3OneToManyChild';
        $level4Collection = 'level4OneToManyChild';

        $database->createCollection($level1Collection);
        $database->createCollection($level2Collection);
        $database->createCollection($level3Collection);
        $database->createCollection($level4Collection);

        $database->createRelationship(new Relationship(collection: $level1Collection, relatedCollection: $level2Collection, type: RelationType::OneToMany, twoWay: true));
        $database->createRelationship(new Relationship(collection: $level2Collection, relatedCollection: $level3Collection, type: RelationType::OneToMany, twoWay: true));
        $database->createRelationship(new Relationship(collection: $level3Collection, relatedCollection: $level4Collection, type: RelationType::OneToMany, twoWay: true));

        $level1 = $database->createDocument($level1Collection, new Document([
            '$id' => 'level1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            $level2Collection => [
                [
                    '$id' => 'level2',
                    $level3Collection => [
                        [
                            '$id' => 'level3',
                            $level4Collection => [
                                [
                                    '$id' => 'level4',
                                ],
                            ],
                        ],
                    ],
                ],
            ],
        ]));
        $this->assertArrayHasKey($level2Collection, $level1);
        $this->assertEquals('level2', $level1[$level2Collection][0]->getId());
        $this->assertArrayHasKey($level3Collection, $level1[$level2Collection][0]);
        $this->assertEquals('level3', $level1[$level2Collection][0][$level3Collection][0]->getId());
        $this->assertArrayNotHasKey($level4Collection, $level1[$level2Collection][0][$level3Collection][0]);

        // Confirm the 4th level document does not exist
        $level3 = $database->getDocument($level3Collection, 'level3');
        $this->assertEquals(0, count($level3[$level4Collection]));

        // Create level 4 document
        $level3->setAttribute($level4Collection, [new Document([
            '$id' => 'level4',
        ])]);
        $level3 = $database->updateDocument($level3Collection, $level3->getId(), $level3);
        $this->assertEquals('level4', $level3[$level4Collection][0]->getId());

        // Verify level 4 document is set
        $level3 = $database->getDocument($level3Collection, 'level3');
        $this->assertArrayHasKey($level4Collection, $level3);
        $this->assertEquals('level4', $level3[$level4Collection][0]->getId());

        // Exceed fetch depth
        $level4 = $database->getDocument($level4Collection, 'level4');
        $this->assertArrayHasKey($level3Collection, $level4);
        $this->assertEquals('level3', $level4[$level3Collection]->getId());
        $this->assertArrayHasKey($level2Collection, $level4[$level3Collection]);
        $this->assertEquals('level2', $level4[$level3Collection][$level2Collection]->getId());
        $this->assertArrayNotHasKey($level1Collection, $level4[$level3Collection][$level2Collection]);
    }

    public function testOneToManyRelationshipKeyWithSymbols(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! $database->getAdapter()->supports(Capability::Relationships)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection('$symbols_coll.ection3');
        $database->createCollection('$symbols_coll.ection4');

        $database->createRelationship(new Relationship(collection: '$symbols_coll.ection3', relatedCollection: '$symbols_coll.ection4', type: RelationType::OneToMany, twoWay: true));

        $doc1 = $database->createDocument('$symbols_coll.ection4', new Document([
            '$id' => ID::unique(),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
        ]));
        $doc2 = $database->createDocument('$symbols_coll.ection3', new Document([
            '$id' => ID::unique(),
            'symbols_collection4' => [$doc1->getId()],
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
        ]));

        $doc1 = $database->getDocument('$symbols_coll.ection4', $doc1->getId());
        $doc2 = $database->getDocument('$symbols_coll.ection3', $doc2->getId());

        $this->assertEquals($doc2->getId(), $doc1->getAttribute('symbols_collection3')->getId());
        /** @var array<Document> $_arr_symbols_collection4_1487 */
        $_arr_symbols_collection4_1487 = $doc2->getAttribute('symbols_collection4');
        $this->assertEquals($doc1->getId(), $_arr_symbols_collection4_1487[0]->getId());
    }

    public function testRecreateOneToManyOneWayRelationshipFromChild(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! $database->getAdapter()->supports(Capability::Relationships)) {
            $this->expectNotToPerformAssertions();

            return;
        }
        $database->createCollection('one', [
            new Attribute(key: 'name', type: ColumnType::String, size: 100, required: false, default: null, signed: true, array: false, format: '', filters: []),
        ], [], [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ]);
        $database->createCollection('two', [
            new Attribute(key: 'name', type: ColumnType::String, size: 100, required: false, default: null, signed: true, array: false, format: '', filters: []),
        ], [], [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ]);

        $database->createRelationship(new Relationship(collection: 'one', relatedCollection: 'two', type: RelationType::OneToMany));

        $database->deleteRelationship('two', 'one');

        $result = $database->createRelationship(new Relationship(collection: 'one', relatedCollection: 'two', type: RelationType::OneToMany));

        $this->assertTrue($result);

        $database->deleteCollection('one');
        $database->deleteCollection('two');
    }

    public function testRecreateOneToManyTwoWayRelationshipFromParent(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! $database->getAdapter()->supports(Capability::Relationships)) {
            $this->expectNotToPerformAssertions();

            return;
        }
        $database->createCollection('one', [
            new Attribute(key: 'name', type: ColumnType::String, size: 100, required: false, default: null, signed: true, array: false, format: '', filters: []),
        ], [], [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ]);
        $database->createCollection('two', [
            new Attribute(key: 'name', type: ColumnType::String, size: 100, required: false, default: null, signed: true, array: false, format: '', filters: []),
        ], [], [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ]);

        $database->createRelationship(new Relationship(collection: 'one', relatedCollection: 'two', type: RelationType::OneToMany, twoWay: true));

        $database->deleteRelationship('one', 'two');

        $result = $database->createRelationship(new Relationship(collection: 'one', relatedCollection: 'two', type: RelationType::OneToMany, twoWay: true));

        $this->assertTrue($result);

        $database->deleteCollection('one');
        $database->deleteCollection('two');
    }

    public function testRecreateOneToManyTwoWayRelationshipFromChild(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! $database->getAdapter()->supports(Capability::Relationships)) {
            $this->expectNotToPerformAssertions();

            return;
        }
        $database->createCollection('one', [
            new Attribute(key: 'name', type: ColumnType::String, size: 100, required: false, default: null, signed: true, array: false, format: '', filters: []),
        ], [], [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ]);
        $database->createCollection('two', [
            new Attribute(key: 'name', type: ColumnType::String, size: 100, required: false, default: null, signed: true, array: false, format: '', filters: []),
        ], [], [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ]);

        $database->createRelationship(new Relationship(collection: 'one', relatedCollection: 'two', type: RelationType::OneToMany, twoWay: true));

        $database->deleteRelationship('two', 'one');

        $result = $database->createRelationship(new Relationship(collection: 'one', relatedCollection: 'two', type: RelationType::OneToMany, twoWay: true));

        $this->assertTrue($result);

        $database->deleteCollection('one');
        $database->deleteCollection('two');
    }

    public function testRecreateOneToManyOneWayRelationshipFromParent(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! $database->getAdapter()->supports(Capability::Relationships)) {
            $this->expectNotToPerformAssertions();

            return;
        }
        $database->createCollection('one', [
            new Attribute(key: 'name', type: ColumnType::String, size: 100, required: false, default: null, signed: true, array: false, format: '', filters: []),
        ], [], [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ]);
        $database->createCollection('two', [
            new Attribute(key: 'name', type: ColumnType::String, size: 100, required: false, default: null, signed: true, array: false, format: '', filters: []),
        ], [], [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ]);

        $database->createRelationship(new Relationship(collection: 'one', relatedCollection: 'two', type: RelationType::OneToMany));

        $database->deleteRelationship('one', 'two');

        $result = $database->createRelationship(new Relationship(collection: 'one', relatedCollection: 'two', type: RelationType::OneToMany));

        $this->assertTrue($result);

        $database->deleteCollection('one');
        $database->deleteCollection('two');
    }

    public function testDeleteBulkDocumentsOneToManyRelationship(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! $database->getAdapter()->supports(Capability::Relationships) || ! $database->getAdapter()->supports(Capability::BatchOperations)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $this->getDatabase()->createCollection('bulk_delete_person_o2m');
        $this->getDatabase()->createCollection('bulk_delete_library_o2m');

        $this->getDatabase()->createAttribute('bulk_delete_person_o2m', new Attribute(key: 'name', type: ColumnType::String, size: 255, required: true));
        $this->getDatabase()->createAttribute('bulk_delete_library_o2m', new Attribute(key: 'name', type: ColumnType::String, size: 255, required: true));
        $this->getDatabase()->createAttribute('bulk_delete_library_o2m', new Attribute(key: 'area', type: ColumnType::String, size: 255, required: true));

        // Restrict
        $this->getDatabase()->createRelationship(new Relationship(collection: 'bulk_delete_person_o2m', relatedCollection: 'bulk_delete_library_o2m', type: RelationType::OneToMany, onDelete: ForeignKeyAction::Restrict));

        $person1 = $this->getDatabase()->createDocument('bulk_delete_person_o2m', new Document([
            '$id' => 'person1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Person 1',
            'bulk_delete_library_o2m' => [
                [
                    '$id' => 'library1',
                    '$permissions' => [
                        Permission::read(Role::any()),
                        Permission::update(Role::any()),
                        Permission::delete(Role::any()),
                    ],
                    'name' => 'Library 1',
                    'area' => 'Area 1',
                ],
                [
                    '$id' => 'library2',
                    '$permissions' => [
                        Permission::read(Role::any()),
                        Permission::update(Role::any()),
                        Permission::delete(Role::any()),
                    ],
                    'name' => 'Library 2',
                    'area' => 'Area 2',
                ],
            ],
        ]));

        $person1 = $this->getDatabase()->getDocument('bulk_delete_person_o2m', 'person1');
        $libraries = $person1->getAttribute('bulk_delete_library_o2m');
        $this->assertCount(2, $libraries);

        // Delete person
        try {
            $this->getDatabase()->deleteDocuments('bulk_delete_person_o2m');
            $this->fail('Failed to throw exception');
        } catch (RestrictedException $e) {
            $this->assertEquals('Cannot delete document because it has at least one related document.', $e->getMessage());
        }

        // Restrict Cleanup
        $this->getDatabase()->deleteDocuments('bulk_delete_library_o2m');
        $this->assertCount(0, $this->getDatabase()->find('bulk_delete_library_o2m'));

        $this->getDatabase()->deleteDocuments('bulk_delete_person_o2m');
        $this->assertCount(0, $this->getDatabase()->find('bulk_delete_person_o2m'));

        // NULL
        $this->getDatabase()->updateRelationship(
            collection: 'bulk_delete_person_o2m',
            id: 'bulk_delete_library_o2m',
            onDelete: ForeignKeyAction::SetNull
        );

        $person1 = $this->getDatabase()->createDocument('bulk_delete_person_o2m', new Document([
            '$id' => 'person1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Person 1',
            'bulk_delete_library_o2m' => [
                [
                    '$id' => 'library1',
                    '$permissions' => [
                        Permission::read(Role::any()),
                        Permission::update(Role::any()),
                        Permission::delete(Role::any()),
                    ],
                    'name' => 'Library 1',
                    'area' => 'Area 1',
                ],
                [
                    '$id' => 'library2',
                    '$permissions' => [
                        Permission::read(Role::any()),
                        Permission::update(Role::any()),
                        Permission::delete(Role::any()),
                    ],
                    'name' => 'Library 2',
                    'area' => 'Area 2',
                ],
            ],
        ]));

        $person1 = $this->getDatabase()->getDocument('bulk_delete_person_o2m', 'person1');
        $libraries = $person1->getAttribute('bulk_delete_library_o2m');
        $this->assertCount(2, $libraries);

        $this->getDatabase()->deleteDocuments('bulk_delete_library_o2m');
        $this->assertCount(0, $this->getDatabase()->find('bulk_delete_library_o2m'));

        $person = $this->getDatabase()->getDocument('bulk_delete_person_o2m', 'person1');
        $libraries = $person->getAttribute('bulk_delete_library_o2m');
        $this->assertEmpty($libraries);

        // NULL - Cleanup
        $this->getDatabase()->deleteDocuments('bulk_delete_person_o2m');
        $this->assertCount(0, $this->getDatabase()->find('bulk_delete_person_o2m'));

        // Cascade
        $this->getDatabase()->updateRelationship(
            collection: 'bulk_delete_person_o2m',
            id: 'bulk_delete_library_o2m',
            onDelete: ForeignKeyAction::Cascade
        );

        $person1 = $this->getDatabase()->createDocument('bulk_delete_person_o2m', new Document([
            '$id' => 'person1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Person 1',
            'bulk_delete_library_o2m' => [
                [
                    '$id' => 'library1',
                    '$permissions' => [
                        Permission::read(Role::any()),
                        Permission::update(Role::any()),
                        Permission::delete(Role::any()),
                    ],
                    'name' => 'Library 1',
                    'area' => 'Area 1',
                ],
                [
                    '$id' => 'library2',
                    '$permissions' => [
                        Permission::read(Role::any()),
                        Permission::update(Role::any()),
                        Permission::delete(Role::any()),
                    ],
                    'name' => 'Library 2',
                    'area' => 'Area 2',
                ],
            ],
        ]));

        $person1 = $this->getDatabase()->getDocument('bulk_delete_person_o2m', 'person1');
        $libraries = $person1->getAttribute('bulk_delete_library_o2m');
        $this->assertCount(2, $libraries);

        $this->getDatabase()->deleteDocuments('bulk_delete_library_o2m');
        $this->assertCount(0, $this->getDatabase()->find('bulk_delete_library_o2m'));

        $person = $this->getDatabase()->getDocument('bulk_delete_person_o2m', 'person1');
        $libraries = $person->getAttribute('bulk_delete_library_o2m');
        $this->assertEmpty($libraries);
    }

    public function testOneToManyAndManyToOneDeleteRelationship(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! $database->getAdapter()->supports(Capability::Relationships)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection('relation1');
        $database->createCollection('relation2');

        $database->createRelationship(new Relationship(collection: 'relation1', relatedCollection: 'relation2', type: RelationType::OneToMany));

        $relation1 = $database->getCollection('relation1');
        /** @var array<mixed> $_ac_attributes_1840 */
        $_ac_attributes_1840 = $relation1->getAttribute('attributes');
        $this->assertCount(1, $_ac_attributes_1840);
        /** @var array<mixed> $_ac_indexes_1841 */
        $_ac_indexes_1841 = $relation1->getAttribute('indexes');
        $this->assertCount(0, $_ac_indexes_1841);
        $relation2 = $database->getCollection('relation2');
        /** @var array<mixed> $_ac_attributes_1843 */
        $_ac_attributes_1843 = $relation2->getAttribute('attributes');
        $this->assertCount(1, $_ac_attributes_1843);
        /** @var array<mixed> $_ac_indexes_1844 */
        $_ac_indexes_1844 = $relation2->getAttribute('indexes');
        $this->assertCount(1, $_ac_indexes_1844);

        $database->deleteRelationship('relation2', 'relation1');

        $relation1 = $database->getCollection('relation1');
        /** @var array<mixed> $_ac_attributes_1849 */
        $_ac_attributes_1849 = $relation1->getAttribute('attributes');
        $this->assertCount(0, $_ac_attributes_1849);
        /** @var array<mixed> $_ac_indexes_1850 */
        $_ac_indexes_1850 = $relation1->getAttribute('indexes');
        $this->assertCount(0, $_ac_indexes_1850);
        $relation2 = $database->getCollection('relation2');
        /** @var array<mixed> $_ac_attributes_1852 */
        $_ac_attributes_1852 = $relation2->getAttribute('attributes');
        $this->assertCount(0, $_ac_attributes_1852);
        /** @var array<mixed> $_ac_indexes_1853 */
        $_ac_indexes_1853 = $relation2->getAttribute('indexes');
        $this->assertCount(0, $_ac_indexes_1853);

        $database->createRelationship(new Relationship(collection: 'relation1', relatedCollection: 'relation2', type: RelationType::ManyToOne));

        $relation1 = $database->getCollection('relation1');
        /** @var array<mixed> $_ac_attributes_1858 */
        $_ac_attributes_1858 = $relation1->getAttribute('attributes');
        $this->assertCount(1, $_ac_attributes_1858);
        /** @var array<mixed> $_ac_indexes_1859 */
        $_ac_indexes_1859 = $relation1->getAttribute('indexes');
        $this->assertCount(1, $_ac_indexes_1859);
        $relation2 = $database->getCollection('relation2');
        /** @var array<mixed> $_ac_attributes_1861 */
        $_ac_attributes_1861 = $relation2->getAttribute('attributes');
        $this->assertCount(1, $_ac_attributes_1861);
        /** @var array<mixed> $_ac_indexes_1862 */
        $_ac_indexes_1862 = $relation2->getAttribute('indexes');
        $this->assertCount(0, $_ac_indexes_1862);

        $database->deleteRelationship('relation1', 'relation2');

        $relation1 = $database->getCollection('relation1');
        /** @var array<mixed> $_ac_attributes_1867 */
        $_ac_attributes_1867 = $relation1->getAttribute('attributes');
        $this->assertCount(0, $_ac_attributes_1867);
        /** @var array<mixed> $_ac_indexes_1868 */
        $_ac_indexes_1868 = $relation1->getAttribute('indexes');
        $this->assertCount(0, $_ac_indexes_1868);
        $relation2 = $database->getCollection('relation2');
        /** @var array<mixed> $_ac_attributes_1870 */
        $_ac_attributes_1870 = $relation2->getAttribute('attributes');
        $this->assertCount(0, $_ac_attributes_1870);
        /** @var array<mixed> $_ac_indexes_1871 */
        $_ac_indexes_1871 = $relation2->getAttribute('indexes');
        $this->assertCount(0, $_ac_indexes_1871);
    }

    public function testUpdateParentAndChild_OneToMany(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (
            ! $database->getAdapter()->supports(Capability::Relationships) ||
            ! $database->getAdapter()->supports(Capability::BatchOperations)
        ) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $parentCollection = 'parent_combined_o2m';
        $childCollection = 'child_combined_o2m';

        $database->createCollection($parentCollection);
        $database->createCollection($childCollection);

        $database->createAttribute($parentCollection, new Attribute(key: 'name', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute($childCollection, new Attribute(key: 'name', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute($childCollection, new Attribute(key: 'parentNumber', type: ColumnType::Integer, size: 0, required: false));

        $database->createRelationship(new Relationship(collection: $parentCollection, relatedCollection: $childCollection, type: RelationType::OneToMany, key: 'parentNumber'));

        $database->createDocument($parentCollection, new Document([
            '$id' => 'parent1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Parent 1',
        ]));

        $database->createDocument($childCollection, new Document([
            '$id' => 'child1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Child 1',
            'parentNumber' => null,
        ]));

        $database->updateDocuments(
            $parentCollection,
            new Document(['name' => 'Parent 1 Updated']),
            [Query::equal('$id', ['parent1'])]
        );

        $parentDoc = $database->getDocument($parentCollection, 'parent1');
        $this->assertEquals('Parent 1 Updated', $parentDoc->getAttribute('name'), 'Parent should be updated');

        $childDoc = $database->getDocument($childCollection, 'child1');
        $this->assertEquals('Child 1', $childDoc->getAttribute('name'), 'Child should remain unchanged');

        // invalid update to child
        try {
            $database->updateDocuments(
                $childCollection,
                new Document(['parentNumber' => 'not-a-number']),
                [Query::equal('$id', ['child1'])]
            );
            $this->fail('Expected exception was not thrown for invalid parentNumber type');
        } catch (\Throwable $e) {
            $this->assertInstanceOf(Structure::class, $e);
        }

        // parent remains unaffected
        $parentDocAfter = $database->getDocument($parentCollection, 'parent1');
        $this->assertEquals('Parent 1 Updated', $parentDocAfter->getAttribute('name'), 'Parent should not be affected by failed child update');

        $database->deleteCollection($parentCollection);
        $database->deleteCollection($childCollection);
    }

    public function testDeleteDocumentsRelationshipErrorDoesNotDeleteParent_OneToMany(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! $database->getAdapter()->supports(Capability::Relationships) || ! $database->getAdapter()->supports(Capability::BatchOperations)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $parentCollection = 'parent_relationship_error_one_to_many';
        $childCollection = 'child_relationship_error_one_to_many';

        $database->createCollection($parentCollection);
        $database->createCollection($childCollection);
        $database->createAttribute($parentCollection, new Attribute(key: 'name', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute($childCollection, new Attribute(key: 'name', type: ColumnType::String, size: 255, required: true));

        $database->createRelationship(new Relationship(collection: $parentCollection, relatedCollection: $childCollection, type: RelationType::OneToMany, onDelete: ForeignKeyAction::Restrict));

        $parent = $database->createDocument($parentCollection, new Document([
            '$id' => 'parent1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Parent 1',
            $childCollection => [
                [
                    '$id' => 'child1',
                    '$permissions' => [
                        Permission::read(Role::any()),
                        Permission::update(Role::any()),
                        Permission::delete(Role::any()),
                    ],
                    'name' => 'Child 1',
                ],
            ],
        ]));

        try {
            $database->deleteDocuments($parentCollection, [Query::equal('$id', ['parent1'])]);
            $this->fail('Expected exception was not thrown');
        } catch (RestrictedException $e) {
            $this->assertEquals('Cannot delete document because it has at least one related document.', $e->getMessage());
        }
        $parentDoc = $database->getDocument($parentCollection, 'parent1');
        $childDoc = $database->getDocument($childCollection, 'child1');
        $this->assertFalse($parentDoc->isEmpty(), 'Parent should not be deleted');
        $this->assertFalse($childDoc->isEmpty(), 'Child should not be deleted');
        $database->deleteCollection($parentCollection);
        $database->deleteCollection($childCollection);
    }

    public function testPartialBatchUpdateWithRelationships(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (! $database->getAdapter()->supports(Capability::Relationships) || ! $database->getAdapter()->supports(Capability::BatchOperations)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        // Setup collections with relationships
        $database->createCollection('products');
        $database->createCollection('categories');

        $database->createAttribute('products', new Attribute(key: 'name', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute('products', new Attribute(key: 'price', type: ColumnType::Double, size: 0, required: true));
        $database->createAttribute('categories', new Attribute(key: 'name', type: ColumnType::String, size: 255, required: true));

        $database->createRelationship(new Relationship(collection: 'categories', relatedCollection: 'products', type: RelationType::OneToMany, twoWay: true, key: 'products', twoWayKey: 'category'));

        // Create category with products
        $database->createDocument('categories', new Document([
            '$id' => 'electronics',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'name' => 'Electronics',
            'products' => [
                [
                    '$id' => 'product1',
                    '$permissions' => [
                        Permission::read(Role::any()),
                        Permission::update(Role::any()),
                    ],
                    'name' => 'Laptop',
                    'price' => 999.99,
                ],
                [
                    '$id' => 'product2',
                    '$permissions' => [
                        Permission::read(Role::any()),
                        Permission::update(Role::any()),
                    ],
                    'name' => 'Mouse',
                    'price' => 25.50,
                ],
            ],
        ]));

        // Verify initial state
        $product1 = $database->getDocument('products', 'product1');
        $this->assertEquals('Laptop', $product1->getAttribute('name'));
        $this->assertEquals(999.99, $product1->getAttribute('price'));
        $this->assertEquals('electronics', $product1->getAttribute('category')->getId());

        $product2 = $database->getDocument('products', 'product2');
        $this->assertEquals('Mouse', $product2->getAttribute('name'));
        $this->assertEquals(25.50, $product2->getAttribute('price'));
        $this->assertEquals('electronics', $product2->getAttribute('category')->getId());

        // Perform a BATCH partial update - ONLY update price, NOT the category relationship
        // This is the critical test case - batch updates with relationships
        $database->updateDocuments(
            'products',
            new Document([
                'price' => 50.00, // Update price for all matching products
                // NOTE: We deliberately do NOT include the 'category' field here - this is a partial update
            ]),
            [Query::equal('$id', ['product1', 'product2'])]
        );

        // Verify that prices were updated but category relationships were preserved
        $product1After = $database->getDocument('products', 'product1');
        $this->assertEquals('Laptop', $product1After->getAttribute('name'), 'Product name should be preserved');
        $this->assertEquals(50.00, $product1After->getAttribute('price'), 'Price should be updated');

        // This is the critical assertion - the category relationship should still exist after batch partial update
        $categoryAfter = $product1After->getAttribute('category');
        $this->assertNotNull($categoryAfter, 'Category relationship should be preserved after batch partial update');
        $this->assertEquals('electronics', $categoryAfter->getId(), 'Category should still be electronics');

        $product2After = $database->getDocument('products', 'product2');
        $this->assertEquals('Mouse', $product2After->getAttribute('name'), 'Product name should be preserved');
        $this->assertEquals(50.00, $product2After->getAttribute('price'), 'Price should be updated');
        $this->assertEquals('electronics', $product2After->getAttribute('category')->getId(), 'Category should still be electronics');

        // Verify the reverse relationship is still intact
        $category = $database->getDocument('categories', 'electronics');
        /** @var array<\Utopia\Database\Document> $products */
        $products = $category->getAttribute('products');
        $this->assertCount(2, $products, 'Category should still have 2 products');
        $this->assertEquals('product1', $products[0]->getId());
        $this->assertEquals('product2', $products[1]->getId());

        $database->deleteCollection('products');
        $database->deleteCollection('categories');
    }

    public function testPartialUpdateOnlyRelationship(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (! $database->getAdapter()->supports(Capability::Relationships)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        // Cleanup any leftover collections from prior failed runs
        if (! $database->getCollection('authors')->isEmpty()) {
            $database->deleteCollection('authors');
        }
        if (! $database->getCollection('books')->isEmpty()) {
            $database->deleteCollection('books');
        }

        // Setup collections
        $database->createCollection('authors');
        $database->createCollection('books');

        $database->createAttribute('authors', new Attribute(key: 'name', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute('authors', new Attribute(key: 'bio', type: ColumnType::String, size: 1000, required: false));
        $database->createAttribute('books', new Attribute(key: 'title', type: ColumnType::String, size: 255, required: true));

        $database->createRelationship(new Relationship(collection: 'authors', relatedCollection: 'books', type: RelationType::OneToMany, twoWay: true, key: 'books', twoWayKey: 'author'));

        // Create author with one book
        $database->createDocument('authors', new Document([
            '$id' => 'author1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'name' => 'John Doe',
            'bio' => 'A great author',
            'books' => [
                [
                    '$id' => 'book1',
                    '$permissions' => [
                        Permission::read(Role::any()),
                        Permission::update(Role::any()),
                    ],
                    'title' => 'First Book',
                ],
            ],
        ]));

        // Create a second book independently
        $database->createDocument('books', new Document([
            '$id' => 'book2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'title' => 'Second Book',
        ]));

        // Verify initial state
        $author = $database->getDocument('authors', 'author1');
        $this->assertEquals('John Doe', $author->getAttribute('name'));
        $this->assertEquals('A great author', $author->getAttribute('bio'));
        /** @var array<mixed> $_ac_books_2164 */
        $_ac_books_2164 = $author->getAttribute('books');
        $this->assertCount(1, $_ac_books_2164);
        /** @var array<Document> $_arr_books_2165 */
        $_arr_books_2165 = $author->getAttribute('books');
        $this->assertEquals('book1', $_arr_books_2165[0]->getId());

        // Partial update that ONLY changes the relationship (adds book2 to the author)
        // Do NOT update name or bio
        $database->updateDocument('authors', 'author1', new Document([
            '$id' => 'author1',
            '$collection' => 'authors',
            'books' => ['book1', 'book2'], // Update relationship
            // NOTE: We deliberately do NOT include 'name' or 'bio'
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
        ]));

        // Verify that the relationship was updated but other fields preserved
        $authorAfter = $database->getDocument('authors', 'author1');
        $this->assertEquals('John Doe', $authorAfter->getAttribute('name'), 'Name should be preserved');
        $this->assertEquals('A great author', $authorAfter->getAttribute('bio'), 'Bio should be preserved');
        $this->assertCount(2, $authorAfter->getAttribute('books'), 'Should now have 2 books');

        /** @var array<Document> $_map_books_2186 */
        $_map_books_2186 = $authorAfter->getAttribute('books');
        $bookIds = \array_map(fn ($book) => $book->getId(), $_map_books_2186);
        $this->assertContains('book1', $bookIds);
        $this->assertContains('book2', $bookIds);

        // Verify reverse relationships
        $book1 = $database->getDocument('books', 'book1');
        $this->assertEquals('author1', $book1->getAttribute('author')->getId());

        $book2 = $database->getDocument('books', 'book2');
        $this->assertEquals('author1', $book2->getAttribute('author')->getId());

        $database->deleteCollection('authors');
        $database->deleteCollection('books');
    }

    public function testPartialUpdateBothDataAndRelationship(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (! $database->getAdapter()->supports(Capability::Relationships)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        // Cleanup any leftover collections from prior failed runs
        if (! $database->getCollection('teams')->isEmpty()) {
            $database->deleteCollection('teams');
        }
        if (! $database->getCollection('players')->isEmpty()) {
            $database->deleteCollection('players');
        }

        // Setup collections
        $database->createCollection('teams');
        $database->createCollection('players');

        $database->createAttribute('teams', new Attribute(key: 'name', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute('teams', new Attribute(key: 'city', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute('teams', new Attribute(key: 'founded', type: ColumnType::Integer, size: 0, required: false));
        $database->createAttribute('players', new Attribute(key: 'name', type: ColumnType::String, size: 255, required: true));

        $database->createRelationship(new Relationship(collection: 'teams', relatedCollection: 'players', type: RelationType::OneToMany, twoWay: true, key: 'players', twoWayKey: 'team'));

        // Create team with players
        $database->createDocument('teams', new Document([
            '$id' => 'team1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'name' => 'The Warriors',
            'city' => 'San Francisco',
            'founded' => 1946,
            'players' => [
                [
                    '$id' => 'player1',
                    '$permissions' => [
                        Permission::read(Role::any()),
                        Permission::update(Role::any()),
                    ],
                    'name' => 'Player One',
                ],
                [
                    '$id' => 'player2',
                    '$permissions' => [
                        Permission::read(Role::any()),
                        Permission::update(Role::any()),
                    ],
                    'name' => 'Player Two',
                ],
            ],
        ]));

        // Create an additional player
        $database->createDocument('players', new Document([
            '$id' => 'player3',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'name' => 'Player Three',
        ]));

        // Verify initial state
        $team = $database->getDocument('teams', 'team1');
        $this->assertEquals('The Warriors', $team->getAttribute('name'));
        $this->assertEquals('San Francisco', $team->getAttribute('city'));
        $this->assertEquals(1946, $team->getAttribute('founded'));
        /** @var array<mixed> $_ac_players_2268 */
        $_ac_players_2268 = $team->getAttribute('players');
        $this->assertCount(2, $_ac_players_2268);

        // Partial update that changes BOTH flat data (city) AND relationship (players)
        // Do NOT update name or founded
        $database->updateDocument('teams', 'team1', new Document([
            '$id' => 'team1',
            '$collection' => 'teams',
            'city' => 'Oakland', // Update flat data
            'players' => ['player1', 'player3'], // Update relationship (replace player2 with player3)
            // NOTE: We deliberately do NOT include 'name' or 'founded'
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
        ]));

        // Verify that both updates worked and other fields preserved
        $teamAfter = $database->getDocument('teams', 'team1');
        $this->assertEquals('The Warriors', $teamAfter->getAttribute('name'), 'Name should be preserved');
        $this->assertEquals('Oakland', $teamAfter->getAttribute('city'), 'City should be updated');
        $this->assertEquals(1946, $teamAfter->getAttribute('founded'), 'Founded should be preserved');
        $this->assertCount(2, $teamAfter->getAttribute('players'), 'Should still have 2 players');

        /** @var array<Document> $_map_players_2291 */
        $_map_players_2291 = $teamAfter->getAttribute('players');
        $playerIds = \array_map(fn ($player) => $player->getId(), $_map_players_2291);
        $this->assertContains('player1', $playerIds, 'Should still have player1');
        $this->assertContains('player3', $playerIds, 'Should now have player3');
        $this->assertNotContains('player2', $playerIds, 'Should no longer have player2');

        // Verify reverse relationships
        $player1 = $database->getDocument('players', 'player1');
        $this->assertEquals('team1', $player1->getAttribute('team')->getId());

        $player2 = $database->getDocument('players', 'player2');
        $this->assertNull($player2->getAttribute('team'), 'Player2 should no longer have a team');

        $player3 = $database->getDocument('players', 'player3');
        $this->assertEquals('team1', $player3->getAttribute('team')->getId());

        $database->deleteCollection('teams');
        $database->deleteCollection('players');
    }

    public function testPartialUpdateOneToManyChildSide(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (! $database->getAdapter()->supports(Capability::Relationships)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection('blogs');
        $database->createCollection('posts');

        $database->createAttribute('blogs', new Attribute(key: 'title', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute('blogs', new Attribute(key: 'description', type: ColumnType::String, size: 1000, required: false));
        $database->createAttribute('posts', new Attribute(key: 'title', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute('posts', new Attribute(key: 'views', type: ColumnType::Integer, size: 0, required: false));

        $database->createRelationship(new Relationship(collection: 'blogs', relatedCollection: 'posts', type: RelationType::OneToMany, twoWay: true, key: 'posts', twoWayKey: 'blog'));

        // Create blog with posts
        $database->createDocument('blogs', new Document([
            '$id' => 'blog1',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'title' => 'Tech Blog',
            'description' => 'A blog about technology',
            'posts' => [
                ['$id' => 'post1', '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())], 'title' => 'Post 1', 'views' => 100],
            ],
        ]));

        // Partial update from child (post) side - update views only, preserve blog relationship
        $database->updateDocument('posts', 'post1', new Document([
            '$id' => 'post1',
            '$collection' => 'posts',
            'views' => 200,
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
        ]));

        $post = $database->getDocument('posts', 'post1');
        $this->assertEquals('Post 1', $post->getAttribute('title'), 'Title should be preserved');
        $this->assertEquals(200, $post->getAttribute('views'), 'Views should be updated');
        $this->assertEquals('blog1', $post->getAttribute('blog')->getId(), 'Blog relationship should be preserved');

        $database->deleteCollection('blogs');
        $database->deleteCollection('posts');
    }

    public function testPartialUpdateWithStringIdsVsDocuments(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (! $database->getAdapter()->supports(Capability::Relationships)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection('libraries');
        $database->createCollection('books_lib');

        $database->createAttribute('libraries', new Attribute(key: 'name', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute('libraries', new Attribute(key: 'location', type: ColumnType::String, size: 255, required: false));
        $database->createAttribute('books_lib', new Attribute(key: 'title', type: ColumnType::String, size: 255, required: true));

        $database->createRelationship(new Relationship(collection: 'libraries', relatedCollection: 'books_lib', type: RelationType::OneToMany, twoWay: true, key: 'books', twoWayKey: 'library'));

        // Create library with books
        $database->createDocument('libraries', new Document([
            '$id' => 'lib1',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'name' => 'Central Library',
            'location' => 'Downtown',
            'books' => [
                ['$id' => 'book1', '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())], 'title' => 'Book One'],
            ],
        ]));

        // Create standalone book
        $database->createDocument('books_lib', new Document([
            '$id' => 'book2',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'title' => 'Book Two',
        ]));

        // Partial update using STRING IDs for relationship
        $database->updateDocument('libraries', 'lib1', new Document([
            '$id' => 'lib1',
            '$collection' => 'libraries',
            'books' => ['book1', 'book2'], // Using string IDs
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
        ]));

        $lib = $database->getDocument('libraries', 'lib1');
        $this->assertEquals('Central Library', $lib->getAttribute('name'), 'Name should be preserved');
        $this->assertEquals('Downtown', $lib->getAttribute('location'), 'Location should be preserved');
        $this->assertCount(2, $lib->getAttribute('books'), 'Should have 2 books');

        // Create another standalone book
        $database->createDocument('books_lib', new Document([
            '$id' => 'book3',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'title' => 'Book Three',
        ]));

        // Partial update using DOCUMENT OBJECTS for relationship
        $database->updateDocument('libraries', 'lib1', new Document([
            '$id' => 'lib1',
            '$collection' => 'libraries',
            'books' => [ // Using Document objects
                new Document(['$id' => 'book1']),
                new Document(['$id' => 'book3']),
            ],
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
        ]));

        $lib = $database->getDocument('libraries', 'lib1');
        $this->assertEquals('Central Library', $lib->getAttribute('name'), 'Name should be preserved');
        $this->assertEquals('Downtown', $lib->getAttribute('location'), 'Location should be preserved');
        $this->assertCount(2, $lib->getAttribute('books'), 'Should have 2 books');

        /** @var array<Document> $_map_books_2433 */
        $_map_books_2433 = $lib->getAttribute('books');
        $bookIds = \array_map(fn ($book) => $book->getId(), $_map_books_2433);
        $this->assertContains('book1', $bookIds);
        $this->assertContains('book3', $bookIds);

        $database->deleteCollection('libraries');
        $database->deleteCollection('books_lib');
    }

    public function testOneToManyRelationshipWithArrayOperators(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (! $database->getAdapter()->supports(Capability::Relationships)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        if (! $database->getAdapter()->supports(Capability::Operators)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        // Cleanup any leftover collections from previous runs
        try {
            $database->deleteCollection('author');
        } catch (\Throwable $e) {
        }
        try {
            $database->deleteCollection('article');
        } catch (\Throwable $e) {
        }

        $database->createCollection('author');
        $database->createCollection('article');

        $database->createAttribute('author', new Attribute(key: 'name', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute('article', new Attribute(key: 'title', type: ColumnType::String, size: 255, required: true));

        $database->createRelationship(new Relationship(collection: 'author', relatedCollection: 'article', type: RelationType::OneToMany, twoWay: true, key: 'articles', twoWayKey: 'author'));

        // Create some articles
        $article1 = $database->createDocument('article', new Document([
            '$id' => 'article1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'title' => 'Article 1',
        ]));

        $article2 = $database->createDocument('article', new Document([
            '$id' => 'article2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'title' => 'Article 2',
        ]));

        $article3 = $database->createDocument('article', new Document([
            '$id' => 'article3',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'title' => 'Article 3',
        ]));

        // Create author with one article
        $database->createDocument('author', new Document([
            '$id' => 'author1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'name' => 'Author 1',
            'articles' => ['article1'],
        ]));

        // Fetch the document to get relationships (needed for Mirror which may not return relationships on create)
        $author = $database->getDocument('author', 'author1');
        /** @var array<mixed> $_ac_articles_2517 */
        $_ac_articles_2517 = $author->getAttribute('articles');
        $this->assertCount(1, $_ac_articles_2517);
        /** @var array<Document> $_arr_articles_2518 */
        $_arr_articles_2518 = $author->getAttribute('articles');
        $this->assertEquals('article1', $_arr_articles_2518[0]->getId());

        // Test arrayAppend - add articles
        $author = $database->updateDocument('author', 'author1', new Document([
            'articles' => \Utopia\Database\Operator::arrayAppend(['article2']),
        ]));

        $author = $database->getDocument('author', 'author1');
        /** @var array<mixed> $_ac_articles_2526 */
        $_ac_articles_2526 = $author->getAttribute('articles');
        $this->assertCount(2, $_ac_articles_2526);
        /** @var array<Document> $_map_articles_2527 */
        $_map_articles_2527 = $author->getAttribute('articles');
        $articleIds = \array_map(fn ($article) => $article->getId(), $_map_articles_2527);
        $this->assertContains('article1', $articleIds);
        $this->assertContains('article2', $articleIds);

        // Test arrayRemove - remove an article
        $author = $database->updateDocument('author', 'author1', new Document([
            'articles' => \Utopia\Database\Operator::arrayRemove('article1'),
        ]));

        $author = $database->getDocument('author', 'author1');
        /** @var array<mixed> $_ac_articles_2537 */
        $_ac_articles_2537 = $author->getAttribute('articles');
        $this->assertCount(1, $_ac_articles_2537);
        /** @var array<Document> $_map_articles_2538 */
        $_map_articles_2538 = $author->getAttribute('articles');
        $articleIds = \array_map(fn ($article) => $article->getId(), $_map_articles_2538);
        $this->assertNotContains('article1', $articleIds);
        $this->assertContains('article2', $articleIds);

        // Cleanup
        $database->deleteCollection('author');
        $database->deleteCollection('article');
    }

    public function testOneToManyChildSideRejectsArrayOperators(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (! $database->getAdapter()->supports(Capability::Relationships)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        if (! $database->getAdapter()->supports(Capability::Operators)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        // Cleanup any leftover collections from previous runs
        try {
            $database->deleteCollection('parent_o2m');
        } catch (\Throwable $e) {
        }
        try {
            $database->deleteCollection('child_o2m');
        } catch (\Throwable $e) {
        }

        $database->createCollection('parent_o2m');
        $database->createCollection('child_o2m');

        $database->createAttribute('parent_o2m', new Attribute(key: 'name', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute('child_o2m', new Attribute(key: 'title', type: ColumnType::String, size: 255, required: true));

        $database->createRelationship(new Relationship(collection: 'parent_o2m', relatedCollection: 'child_o2m', type: RelationType::OneToMany, twoWay: true, key: 'children', twoWayKey: 'parent'));

        // Create a parent
        $database->createDocument('parent_o2m', new Document([
            '$id' => 'parent1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'name' => 'Parent 1',
        ]));

        // Create child with parent
        $database->createDocument('child_o2m', new Document([
            '$id' => 'child1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'title' => 'Child 1',
            'parent' => 'parent1',
        ]));

        // Array operators should fail on child side (single-value "parent" relationship)
        try {
            $database->updateDocument('child_o2m', 'child1', new Document([
                'parent' => \Utopia\Database\Operator::arrayAppend(['parent2']),
            ]));
            $this->fail('Expected exception for array operator on child side of one-to-many relationship');
        } catch (\Utopia\Database\Exception\Structure $e) {
            $this->assertStringContainsString('single-value relationship', $e->getMessage());
        }

        // Cleanup
        $database->deleteCollection('parent_o2m');
        $database->deleteCollection('child_o2m');
    }
}
