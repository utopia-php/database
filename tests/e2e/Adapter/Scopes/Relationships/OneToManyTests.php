<?php

namespace Tests\E2E\Adapter\Scopes\Relationships;

use Exception;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Restricted as RestrictedException;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;

trait OneToManyTests
{
    public function testOneToManyOneWayRelationship(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $database->createCollection('artist');
        $database->createCollection('album');

        $database->createAttribute('artist', 'name', Database::VAR_STRING, 255, true);
        $database->createAttribute('album', 'name', Database::VAR_STRING, 255, true);
        $database->createAttribute('album', 'price', Database::VAR_FLOAT, 0, true);

        $database->createRelationship(
            collection: 'artist',
            relatedCollection: 'album',
            type: Database::RELATION_ONE_TO_MANY,
            id: 'albums'
        );

        // Check metadata for collection
        $collection = $database->getCollection('artist');
        $attributes = $collection->getAttribute('attributes', []);

        foreach ($attributes as $attribute) {
            if ($attribute['key'] === 'albums') {
                $this->assertEquals('relationship', $attribute['type']);
                $this->assertEquals('albums', $attribute['$id']);
                $this->assertEquals('albums', $attribute['key']);
                $this->assertEquals('album', $attribute['options']['relatedCollection']);
                $this->assertEquals(Database::RELATION_ONE_TO_MANY, $attribute['options']['relationType']);
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
                        Permission::update(Role::any())
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
        $this->assertEquals(1, \count($artist1Document->getAttribute('albums')));

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
                ]
            ]
        ]));

        $documents = $database->find('artist', [
            Query::select(['name']),
            Query::limit(1)
        ]);
        $this->assertArrayNotHasKey('albums', $documents[0]);

        // Get document with relationship
        $artist = $database->getDocument('artist', 'artist1');
        $albums = $artist->getAttribute('albums', []);
        $this->assertEquals('album1', $albums[0]['$id']);
        $this->assertArrayNotHasKey('artist', $albums[0]);

        $artist = $database->getDocument('artist', 'artist2');
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
            Query::select(['*', 'albums.name'])
        ]);

        if ($artist->isEmpty()) {
            $this->fail('Artist not found');
        }

        $this->assertEquals('Album 1', $artist->getAttribute('albums')[0]->getAttribute('name'));
        $this->assertArrayNotHasKey('price', $artist->getAttribute('albums')[0]);

        $artist = $database->getDocument('artist', 'artist1', [
            Query::select(['*', 'albums.name'])
        ]);

        $this->assertEquals('Album 1', $artist->getAttribute('albums')[0]->getAttribute('name'));
        $this->assertArrayNotHasKey('price', $artist->getAttribute('albums')[0]);

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
        $albums = $artist1->getAttribute('albums', []);
        $albums[0]->setAttribute('name', 'Album 1 Updated');

        $artist1 = $database->updateDocument(
            'artist',
            $artist1->getId(),
            $artist1->setAttribute('albums', $albums)
        );

        $this->assertEquals('Album 1 Updated', $artist1->getAttribute('albums')[0]->getAttribute('name'));
        $artist1 = $database->getDocument('artist', 'artist1');
        $this->assertEquals('Album 1 Updated', $artist1->getAttribute('albums')[0]->getAttribute('name'));

        $albumId = $artist1->getAttribute('albums')[0]->getAttribute('$id');
        $albumDocument = $database->getDocument('album', $albumId);
        $albumDocument->setAttribute('name', 'Album 1 Updated!!!');
        $database->updateDocument('album', $albumDocument->getId(), $albumDocument);
        $albumDocument = $database->getDocument('album', $albumDocument->getId());
        $artist1 = $database->getDocument('artist', $artist1->getId());

        $this->assertEquals('Album 1 Updated!!!', $albumDocument['name']);
        $this->assertEquals($albumDocument->getId(), $artist1->getAttribute('albums')[0]->getId());
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

        $this->assertEquals('Album 3', $artist3->getAttribute('albums')[0]->getAttribute('name'));
        $artist3 = $database->getDocument('artist', 'artist3');
        $this->assertEquals('Album 3', $artist3->getAttribute('albums')[0]->getAttribute('name'));

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
            onDelete: Database::RELATION_MUTATE_SET_NULL
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
            onDelete: Database::RELATION_MUTATE_CASCADE
        );

        // Delete parent, will delete child
        $database->deleteDocument('artist', 'artist2');

        // Check parent and child were deleted
        $library = $database->getDocument('artist', 'artist2');
        $this->assertEquals(true, $library->isEmpty());

        $library = $database->getDocument('album', 'album2');
        $this->assertEquals(true, $library->isEmpty());

        $albums = [];
        for ($i = 1 ; $i <= 50 ; $i++) {
            $albums[] = [
                '$id' => 'album_' . $i,
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'name' => 'album ' . $i . ' ' . 'Artist 100',
                'price' => 100,
            ];
        }

        $artist = $database->createDocument('artist', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Artist 100',
            'newAlbums' => $albums
        ]));

        $artist = $database->getDocument('artist', $artist->getId());
        $this->assertCount(50, $artist->getAttribute('newAlbums'));

        $albums = $database->find('album', [
            Query::equal('artist', [$artist->getId()]),
            Query::limit(999)
        ]);

        $this->assertCount(50, $albums);

        $count = $database->count('album', [
            Query::equal('artist', [$artist->getId()]),
        ]);

        $this->assertEquals(50, $count);

        $database->deleteDocument('album', 'album_1');
        $artist = $database->getDocument('artist', $artist->getId());
        $this->assertCount(49, $artist->getAttribute('newAlbums'));

        $database->deleteDocument('artist', $artist->getId());

        $albums = $database->find('album', [
            Query::equal('artist', [$artist->getId()]),
            Query::limit(999)
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
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $database->createCollection('customer');
        $database->createCollection('account');

        $database->createAttribute('customer', 'name', Database::VAR_STRING, 255, true);
        $database->createAttribute('account', 'name', Database::VAR_STRING, 255, true);
        $database->createAttribute('account', 'number', Database::VAR_STRING, 255, true);

        $database->createRelationship(
            collection: 'customer',
            relatedCollection: 'account',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            id: 'accounts'
        );

        // Check metadata for collection
        $collection = $database->getCollection('customer');
        $attributes = $collection->getAttribute('attributes', []);
        foreach ($attributes as $attribute) {
            if ($attribute['key'] === 'accounts') {
                $this->assertEquals('relationship', $attribute['type']);
                $this->assertEquals('accounts', $attribute['$id']);
                $this->assertEquals('accounts', $attribute['key']);
                $this->assertEquals('account', $attribute['options']['relatedCollection']);
                $this->assertEquals(Database::RELATION_ONE_TO_MANY, $attribute['options']['relationType']);
                $this->assertEquals(true, $attribute['options']['twoWay']);
                $this->assertEquals('customer', $attribute['options']['twoWayKey']);
            }
        }

        // Check metadata for related collection
        $collection = $database->getCollection('account');
        $attributes = $collection->getAttribute('attributes', []);
        foreach ($attributes as $attribute) {
            if ($attribute['key'] === 'customer') {
                $this->assertEquals('relationship', $attribute['type']);
                $this->assertEquals('customer', $attribute['$id']);
                $this->assertEquals('customer', $attribute['key']);
                $this->assertEquals('customer', $attribute['options']['relatedCollection']);
                $this->assertEquals(Database::RELATION_ONE_TO_MANY, $attribute['options']['relationType']);
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
        $database->updateDocument('customer', 'customer1', $customer1->setAttribute('accounts', ['account1','no-account']));

        $customer1Document = $database->getDocument('customer', 'customer1');
        // Assert document does not contain non existing relation document.
        $this->assertEquals(1, \count($customer1Document->getAttribute('accounts')));

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
                'account2'
            ]
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
                'name' => 'Customer 3'
            ]
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
            'customer' => 'customer4'
        ]));

        // Get documents with relationship
        $customer = $database->getDocument('customer', 'customer1');
        $accounts = $customer->getAttribute('accounts', []);
        $this->assertEquals('account1', $accounts[0]['$id']);
        $this->assertArrayNotHasKey('customer', $accounts[0]);

        $customer = $database->getDocument('customer', 'customer2');
        $accounts = $customer->getAttribute('accounts', []);
        $this->assertEquals('account2', $accounts[0]['$id']);
        $this->assertArrayNotHasKey('customer', $accounts[0]);

        $customer = $database->getDocument('customer', 'customer3');
        $accounts = $customer->getAttribute('accounts', []);
        $this->assertEquals('account3', $accounts[0]['$id']);
        $this->assertArrayNotHasKey('customer', $accounts[0]);

        $customer = $database->getDocument('customer', 'customer4');
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
            Query::select(['*', 'accounts.name'])
        ]);

        if ($customer->isEmpty()) {
            throw new Exception('Customer not found');
        }

        $this->assertEquals('Account 1', $customer->getAttribute('accounts')[0]->getAttribute('name'));
        $this->assertArrayNotHasKey('number', $customer->getAttribute('accounts')[0]);

        $customer = $database->getDocument('customer', 'customer1', [
            Query::select(['*', 'accounts.name'])
        ]);

        $this->assertEquals('Account 1', $customer->getAttribute('accounts')[0]->getAttribute('name'));
        $this->assertArrayNotHasKey('number', $customer->getAttribute('accounts')[0]);

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
        $accounts = $customer1->getAttribute('accounts', []);
        $accounts[0]->setAttribute('name', 'Account 1 Updated');

        $customer1 = $database->updateDocument(
            'customer',
            $customer1->getId(),
            $customer1->setAttribute('accounts', $accounts)
        );

        $this->assertEquals('Account 1 Updated', $customer1->getAttribute('accounts')[0]->getAttribute('name'));
        $customer1 = $database->getDocument('customer', 'customer1');
        $this->assertEquals('Account 1 Updated', $customer1->getAttribute('accounts')[0]->getAttribute('name'));

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

        $this->assertEquals('Customer 2 Updated', $account2->getAttribute('customer')->getAttribute('name'));
        $account2 = $database->getDocument('account', 'account2');
        $this->assertEquals('Customer 2 Updated', $account2->getAttribute('customer')->getAttribute('name'));

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

        $this->assertEquals('Account 5', $customer5->getAttribute('accounts')[0]->getAttribute('name'));
        $customer5 = $database->getDocument('customer', 'customer5');
        $this->assertEquals('Account 5', $customer5->getAttribute('accounts')[0]->getAttribute('name'));

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

        $this->assertEquals('Customer 6', $account6->getAttribute('customer')->getAttribute('name'));
        $account6 = $database->getDocument('account', 'account6');
        $this->assertEquals('Customer 6', $account6->getAttribute('customer')->getAttribute('name'));

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
            onDelete: Database::RELATION_MUTATE_SET_NULL
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
            onDelete: Database::RELATION_MUTATE_CASCADE
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
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $database->createCollection('countries');
        $database->createCollection('cities');
        $database->createCollection('mayors');

        $database->createAttribute('cities', 'name', Database::VAR_STRING, 255, true);
        $database->createAttribute('countries', 'name', Database::VAR_STRING, 255, true);
        $database->createAttribute('mayors', 'name', Database::VAR_STRING, 255, true);

        $database->createRelationship(
            collection: 'countries',
            relatedCollection: 'cities',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            twoWayKey: 'country'
        );
        $database->createRelationship(
            collection: 'cities',
            relatedCollection: 'mayors',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
            id: 'mayor',
            twoWayKey: 'city'
        );

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
            Query::limit(1)
        ]);
        $this->assertEquals('Mayor 1', $documents[0]['cities'][0]['mayor']['name']);

        $documents = $database->find('countries', [
            Query::select(['name']),
            Query::limit(1)
        ]);
        $this->assertArrayHasKey('name', $documents[0]);
        $this->assertArrayNotHasKey('cities', $documents[0]);

        $documents = $database->find('countries', [
            Query::select(['*']),
            Query::limit(1)
        ]);
        $this->assertArrayHasKey('name', $documents[0]);
        $this->assertArrayNotHasKey('cities', $documents[0]);

        $documents = $database->find('countries', [
            Query::select(['*', 'cities.*', 'cities.mayor.*']),
            Query::limit(1)
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
            'mayor' => 'mayor1', // we don't support partial updates at the moment
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
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $database->createCollection('dormitories');
        $database->createCollection('occupants');
        $database->createCollection('pets');

        $database->createAttribute('dormitories', 'name', Database::VAR_STRING, 255, true);
        $database->createAttribute('occupants', 'name', Database::VAR_STRING, 255, true);
        $database->createAttribute('pets', 'name', Database::VAR_STRING, 255, true);

        $database->createRelationship(
            collection: 'dormitories',
            relatedCollection: 'occupants',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            twoWayKey: 'dormitory'
        );
        $database->createRelationship(
            collection: 'occupants',
            relatedCollection: 'pets',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            twoWayKey: 'occupant'
        );

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
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $database->createCollection('home');
        $database->createCollection('renters');
        $database->createCollection('floors');

        $database->createAttribute('home', 'name', Database::VAR_STRING, 255, true);
        $database->createAttribute('renters', 'name', Database::VAR_STRING, 255, true);
        $database->createAttribute('floors', 'name', Database::VAR_STRING, 255, true);

        $database->createRelationship(
            collection: 'home',
            relatedCollection: 'renters',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true
        );
        $database->createRelationship(
            collection: 'renters',
            relatedCollection: 'floors',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true,
            id: 'floor'
        );

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
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $database->createCollection('owners');
        $database->createCollection('cats');
        $database->createCollection('toys');

        $database->createAttribute('owners', 'name', Database::VAR_STRING, 255, true);
        $database->createAttribute('cats', 'name', Database::VAR_STRING, 255, true);
        $database->createAttribute('toys', 'name', Database::VAR_STRING, 255, true);

        $database->createRelationship(
            collection: 'owners',
            relatedCollection: 'cats',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            twoWayKey: 'owner'
        );
        $database->createRelationship(
            collection: 'cats',
            relatedCollection: 'toys',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true
        );

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
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForRelationships()) {
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

        $database->createRelationship(
            collection: $level1Collection,
            relatedCollection: $level2Collection,
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
        );
        $database->createRelationship(
            collection: $level2Collection,
            relatedCollection: $level3Collection,
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
        );
        $database->createRelationship(
            collection: $level3Collection,
            relatedCollection: $level4Collection,
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
        );

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
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForRelationships()) {
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

        $database->createRelationship(
            collection: $level1Collection,
            relatedCollection: $level2Collection,
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
        );
        $database->createRelationship(
            collection: $level2Collection,
            relatedCollection: $level3Collection,
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
        );
        $database->createRelationship(
            collection: $level3Collection,
            relatedCollection: $level4Collection,
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
        );

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
                            ]
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
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $database->createCollection('$symbols_coll.ection3');
        $database->createCollection('$symbols_coll.ection4');

        $database->createRelationship(
            collection: '$symbols_coll.ection3',
            relatedCollection: '$symbols_coll.ection4',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
        );

        $doc1 = $database->createDocument('$symbols_coll.ection4', new Document([
            '$id' => ID::unique(),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any())
            ]
        ]));
        $doc2 = $database->createDocument('$symbols_coll.ection3', new Document([
            '$id' => ID::unique(),
            '$symbols_coll.ection4' => [$doc1->getId()],
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any())
            ]
        ]));

        $doc1 = $database->getDocument('$symbols_coll.ection4', $doc1->getId());
        $doc2 = $database->getDocument('$symbols_coll.ection3', $doc2->getId());

        $this->assertEquals($doc2->getId(), $doc1->getAttribute('$symbols_coll.ection3')->getId());
        $this->assertEquals($doc1->getId(), $doc2->getAttribute('$symbols_coll.ection4')[0]->getId());
    }

    public function testRecreateOneToManyOneWayRelationshipFromChild(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }
        $database->createCollection('one', [
            new Document([
                '$id' => ID::custom('name'),
                'type' => Database::VAR_STRING,
                'format' => '',
                'size' => 100,
                'signed' => true,
                'required' => false,
                'default' => null,
                'array' => false,
                'filters' => [],
            ]),
        ], [], [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ]);
        $database->createCollection('two', [
            new Document([
                '$id' => ID::custom('name'),
                'type' => Database::VAR_STRING,
                'format' => '',
                'size' => 100,
                'signed' => true,
                'required' => false,
                'default' => null,
                'array' => false,
                'filters' => [],
            ]),
        ], [], [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ]);

        $database->createRelationship(
            collection: 'one',
            relatedCollection: 'two',
            type: Database::RELATION_ONE_TO_MANY,
        );

        $database->deleteRelationship('two', 'one');

        $result = $database->createRelationship(
            collection: 'one',
            relatedCollection: 'two',
            type: Database::RELATION_ONE_TO_MANY,
        );

        $this->assertTrue($result);

        $database->deleteCollection('one');
        $database->deleteCollection('two');
    }

    public function testRecreateOneToManyTwoWayRelationshipFromParent(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }
        $database->createCollection('one', [
            new Document([
                '$id' => ID::custom('name'),
                'type' => Database::VAR_STRING,
                'format' => '',
                'size' => 100,
                'signed' => true,
                'required' => false,
                'default' => null,
                'array' => false,
                'filters' => [],
            ]),
        ], [], [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ]);
        $database->createCollection('two', [
            new Document([
                '$id' => ID::custom('name'),
                'type' => Database::VAR_STRING,
                'format' => '',
                'size' => 100,
                'signed' => true,
                'required' => false,
                'default' => null,
                'array' => false,
                'filters' => [],
            ]),
        ], [], [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ]);

        $database->createRelationship(
            collection: 'one',
            relatedCollection: 'two',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
        );

        $database->deleteRelationship('one', 'two');

        $result = $database->createRelationship(
            collection: 'one',
            relatedCollection: 'two',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
        );

        $this->assertTrue($result);

        $database->deleteCollection('one');
        $database->deleteCollection('two');
    }

    public function testRecreateOneToManyTwoWayRelationshipFromChild(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }
        $database->createCollection('one', [
            new Document([
                '$id' => ID::custom('name'),
                'type' => Database::VAR_STRING,
                'format' => '',
                'size' => 100,
                'signed' => true,
                'required' => false,
                'default' => null,
                'array' => false,
                'filters' => [],
            ]),
        ], [], [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ]);
        $database->createCollection('two', [
            new Document([
                '$id' => ID::custom('name'),
                'type' => Database::VAR_STRING,
                'format' => '',
                'size' => 100,
                'signed' => true,
                'required' => false,
                'default' => null,
                'array' => false,
                'filters' => [],
            ]),
        ], [], [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ]);

        $database->createRelationship(
            collection: 'one',
            relatedCollection: 'two',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
        );

        $database->deleteRelationship('two', 'one');

        $result = $database->createRelationship(
            collection: 'one',
            relatedCollection: 'two',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
        );

        $this->assertTrue($result);

        $database->deleteCollection('one');
        $database->deleteCollection('two');
    }

    public function testRecreateOneToManyOneWayRelationshipFromParent(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }
        $database->createCollection('one', [
            new Document([
                '$id' => ID::custom('name'),
                'type' => Database::VAR_STRING,
                'format' => '',
                'size' => 100,
                'signed' => true,
                'required' => false,
                'default' => null,
                'array' => false,
                'filters' => [],
            ]),
        ], [], [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ]);
        $database->createCollection('two', [
            new Document([
                '$id' => ID::custom('name'),
                'type' => Database::VAR_STRING,
                'format' => '',
                'size' => 100,
                'signed' => true,
                'required' => false,
                'default' => null,
                'array' => false,
                'filters' => [],
            ]),
        ], [], [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ]);

        $database->createRelationship(
            collection: 'one',
            relatedCollection: 'two',
            type: Database::RELATION_ONE_TO_MANY,
        );

        $database->deleteRelationship('one', 'two');

        $result = $database->createRelationship(
            collection: 'one',
            relatedCollection: 'two',
            type: Database::RELATION_ONE_TO_MANY,
        );

        $this->assertTrue($result);

        $database->deleteCollection('one');
        $database->deleteCollection('two');
    }

    public function testDeleteBulkDocumentsOneToManyRelationship(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForRelationships() || !$database->getAdapter()->getSupportForBatchOperations()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->getDatabase()->createCollection('bulk_delete_person_o2m');
        $this->getDatabase()->createCollection('bulk_delete_library_o2m');

        $this->getDatabase()->createAttribute('bulk_delete_person_o2m', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('bulk_delete_library_o2m', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('bulk_delete_library_o2m', 'area', Database::VAR_STRING, 255, true);

        // Restrict
        $this->getDatabase()->createRelationship(
            collection: 'bulk_delete_person_o2m',
            relatedCollection: 'bulk_delete_library_o2m',
            type: Database::RELATION_ONE_TO_MANY,
            onDelete: Database::RELATION_MUTATE_RESTRICT
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
            onDelete: Database::RELATION_MUTATE_SET_NULL
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
            onDelete: Database::RELATION_MUTATE_CASCADE
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
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $database->createCollection('relation1');
        $database->createCollection('relation2');

        $database->createRelationship(
            collection: 'relation1',
            relatedCollection: 'relation2',
            type: Database::RELATION_ONE_TO_MANY,
        );

        $relation1 = $database->getCollection('relation1');
        $this->assertCount(1, $relation1->getAttribute('attributes'));
        $this->assertCount(0, $relation1->getAttribute('indexes'));
        $relation2 = $database->getCollection('relation2');
        $this->assertCount(1, $relation2->getAttribute('attributes'));
        $this->assertCount(1, $relation2->getAttribute('indexes'));

        $database->deleteRelationship('relation2', 'relation1');

        $relation1 = $database->getCollection('relation1');
        $this->assertCount(0, $relation1->getAttribute('attributes'));
        $this->assertCount(0, $relation1->getAttribute('indexes'));
        $relation2 = $database->getCollection('relation2');
        $this->assertCount(0, $relation2->getAttribute('attributes'));
        $this->assertCount(0, $relation2->getAttribute('indexes'));

        $database->createRelationship(
            collection: 'relation1',
            relatedCollection: 'relation2',
            type: Database::RELATION_MANY_TO_ONE,
        );

        $relation1 = $database->getCollection('relation1');
        $this->assertCount(1, $relation1->getAttribute('attributes'));
        $this->assertCount(1, $relation1->getAttribute('indexes'));
        $relation2 = $database->getCollection('relation2');
        $this->assertCount(1, $relation2->getAttribute('attributes'));
        $this->assertCount(0, $relation2->getAttribute('indexes'));

        $database->deleteRelationship('relation1', 'relation2');

        $relation1 = $database->getCollection('relation1');
        $this->assertCount(0, $relation1->getAttribute('attributes'));
        $this->assertCount(0, $relation1->getAttribute('indexes'));
        $relation2 = $database->getCollection('relation2');
        $this->assertCount(0, $relation2->getAttribute('attributes'));
        $this->assertCount(0, $relation2->getAttribute('indexes'));
    }
    public function testDeleteDocumentsRelationshipErrorDoesNotDeleteParent_OneToMany(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForRelationships() || !$database->getAdapter()->getSupportForBatchOperations()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $parentCollection = 'parent_relationship_error_one_to_many';
        $childCollection = 'child_relationship_error_one_to_many';

        $database->createCollection($parentCollection);
        $database->createCollection($childCollection);
        $database->createAttribute($parentCollection, 'name', Database::VAR_STRING, 255, true);
        $database->createAttribute($childCollection, 'name', Database::VAR_STRING, 255, true);

        $database->createRelationship(
            collection: $parentCollection,
            relatedCollection: $childCollection,
            type: Database::RELATION_ONE_TO_MANY,
            onDelete: Database::RELATION_MUTATE_RESTRICT
        );

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
                ]
            ]
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
}
