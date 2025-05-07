<?php

namespace Tests\E2E\Adapter\Scopes\Relationships;

use Exception;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\Restricted as RestrictedException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;

trait OneToOneTests
{
    public function testOneToOneOneWayRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('person');
        static::getDatabase()->createCollection('library');

        static::getDatabase()->createAttribute('person', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('library', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('library', 'area', Database::VAR_STRING, 255, true);

        static::getDatabase()->createRelationship(
            collection: 'person',
            relatedCollection: 'library',
            type: Database::RELATION_ONE_TO_ONE
        );

        // Check metadata for collection
        $collection = static::getDatabase()->getCollection('person');
        $attributes = $collection->getAttribute('attributes', []);

        foreach ($attributes as $attribute) {
            if ($attribute['key'] === 'library') {
                $this->assertEquals('relationship', $attribute['type']);
                $this->assertEquals('library', $attribute['$id']);
                $this->assertEquals('library', $attribute['key']);
                $this->assertEquals('library', $attribute['options']['relatedCollection']);
                $this->assertEquals(Database::RELATION_ONE_TO_ONE, $attribute['options']['relationType']);
                $this->assertEquals(false, $attribute['options']['twoWay']);
                $this->assertEquals('person', $attribute['options']['twoWayKey']);
            }
        }

        try {
            static::getDatabase()->deleteAttribute('person', 'library');
            $this->fail('Failed to throw Exception');
        } catch (Exception $e) {
            $this->assertEquals('Cannot delete relationship as an attribute', $e->getMessage());
        }

        // Create document with relationship with nested data
        $person1 = static::getDatabase()->createDocument('person', new Document([
            '$id' => 'person1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Person 1',
            'library' => [
                '$id' => 'library1',
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'name' => 'Library 1',
                'area' => 'Area 1',
            ],
        ]));

        // Update a document with non existing related document. It should not get added to the list.
        static::getDatabase()->updateDocument(
            'person',
            'person1',
            $person1->setAttribute('library', 'no-library')
        );

        $person1Document = static::getDatabase()->getDocument('person', 'person1');
        // Assert document does not contain non existing relation document.
        $this->assertEquals(null, $person1Document->getAttribute('library'));

        static::getDatabase()->updateDocument(
            'person',
            'person1',
            $person1->setAttribute('library', 'library1')
        );

        // Update through create
        $library10 = static::getDatabase()->createDocument('library', new Document([
            '$id' => 'library10',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            'name' => 'Library 10',
            'area' => 'Area 10',
        ]));
        $person10 = static::getDatabase()->createDocument('person', new Document([
            '$id' => 'person10',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Person 10',
            'library' => [
                '$id' => $library10->getId(),
                'name' => 'Library 10 Updated',
                'area' => 'Area 10 Updated',
            ],
        ]));
        $this->assertEquals('Library 10 Updated', $person10->getAttribute('library')->getAttribute('name'));
        $library10 = static::getDatabase()->getDocument('library', $library10->getId());
        $this->assertEquals('Library 10 Updated', $library10->getAttribute('name'));

        // Create document with relationship with related ID
        static::getDatabase()->createDocument('library', new Document([
            '$id' => 'library2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Library 2',
            'area' => 'Area 2',
        ]));
        static::getDatabase()->createDocument('person', new Document([
            '$id' => 'person2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Person 2',
            'library' => 'library2',
        ]));

        // Get documents with relationship
        $person1 = static::getDatabase()->getDocument('person', 'person1');
        $library = $person1->getAttribute('library');
        $this->assertEquals('library1', $library['$id']);
        $this->assertArrayNotHasKey('person', $library);

        $person = static::getDatabase()->getDocument('person', 'person2');
        $library = $person->getAttribute('library');
        $this->assertEquals('library2', $library['$id']);
        $this->assertArrayNotHasKey('person', $library);

        // Get related documents
        $library = static::getDatabase()->getDocument('library', 'library1');
        $this->assertArrayNotHasKey('person', $library);

        $library = static::getDatabase()->getDocument('library', 'library2');
        $this->assertArrayNotHasKey('person', $library);

        $people = static::getDatabase()->find('person', [
            Query::select(['name'])
        ]);

        $this->assertArrayNotHasKey('library', $people[0]);

        $people = static::getDatabase()->find('person');
        $this->assertEquals(3, \count($people));

        // Select related document attributes
        $person = static::getDatabase()->findOne('person', [
            Query::select(['*', 'library.name'])
        ]);

        if ($person->isEmpty()) {
            throw new Exception('Person not found');
        }

        $this->assertEquals('Library 1', $person->getAttribute('library')->getAttribute('name'));
        $this->assertArrayNotHasKey('area', $person->getAttribute('library'));

        $person = static::getDatabase()->getDocument('person', 'person1', [
            Query::select(['*', 'library.name', '$id'])
        ]);

        $this->assertEquals('Library 1', $person->getAttribute('library')->getAttribute('name'));
        $this->assertArrayNotHasKey('area', $person->getAttribute('library'));



        $document = static::getDatabase()->getDocument('person', $person->getId(), [
            Query::select(['name']),
        ]);
        $this->assertArrayNotHasKey('library', $document);
        $this->assertEquals('Person 1', $document['name']);

        $document = static::getDatabase()->getDocument('person', $person->getId(), [
            Query::select(['*']),
        ]);
        $this->assertEquals('library1', $document['library']);

        $document = static::getDatabase()->getDocument('person', $person->getId(), [
            Query::select(['library.*']),
        ]);
        $this->assertEquals('Library 1', $document['library']['name']);
        $this->assertArrayNotHasKey('name', $document);

        // Update root document attribute without altering relationship
        $person1 = static::getDatabase()->updateDocument(
            'person',
            $person1->getId(),
            $person1->setAttribute('name', 'Person 1 Updated')
        );

        $this->assertEquals('Person 1 Updated', $person1->getAttribute('name'));
        $person1 = static::getDatabase()->getDocument('person', 'person1');
        $this->assertEquals('Person 1 Updated', $person1->getAttribute('name'));

        // Update nested document attribute
        $person1 = static::getDatabase()->updateDocument(
            'person',
            $person1->getId(),
            $person1->setAttribute(
                'library',
                $person1
                    ->getAttribute('library')
                    ->setAttribute('name', 'Library 1 Updated')
            )
        );

        $this->assertEquals('Library 1 Updated', $person1->getAttribute('library')->getAttribute('name'));
        $person1 = static::getDatabase()->getDocument('person', 'person1');
        $this->assertEquals('Library 1 Updated', $person1->getAttribute('library')->getAttribute('name'));

        // Create new document with no relationship
        $person3 = static::getDatabase()->createDocument('person', new Document([
            '$id' => 'person3',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Person 3',
        ]));

        // Update to relate to created document
        $person3 = static::getDatabase()->updateDocument(
            'person',
            $person3->getId(),
            $person3->setAttribute('library', new Document([
                '$id' => 'library3',
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                ],
                'name' => 'Library 3',
                'area' => 'Area 3',
            ]))
        );

        $this->assertEquals('library3', $person3->getAttribute('library')['$id']);
        $person3 = static::getDatabase()->getDocument('person', 'person3');
        $this->assertEquals('Library 3', $person3['library']['name']);

        $libraryDocument = static::getDatabase()->getDocument('library', 'library3');
        $libraryDocument->setAttribute('name', 'Library 3 updated');
        static::getDatabase()->updateDocument('library', 'library3', $libraryDocument);
        $libraryDocument = static::getDatabase()->getDocument('library', 'library3');
        $this->assertEquals('Library 3 updated', $libraryDocument['name']);

        $person3 = static::getDatabase()->getDocument('person', 'person3');
        // Todo: This is failing
        $this->assertEquals($libraryDocument['name'], $person3['library']['name']);
        $this->assertEquals('library3', $person3->getAttribute('library')['$id']);

        // One to one can't relate to multiple documents, unique index throws duplicate
        try {
            static::getDatabase()->updateDocument(
                'person',
                $person1->getId(),
                $person1->setAttribute('library', 'library2')
            );
            $this->fail('Failed to throw duplicate exception');
        } catch (Exception $e) {
            $this->assertInstanceOf(DuplicateException::class, $e);
        }

        // Create new document
        $library4 = static::getDatabase()->createDocument('library', new Document([
            '$id' => 'library4',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Library 4',
            'area' => 'Area 4',
        ]));

        // Relate existing document to new document
        static::getDatabase()->updateDocument(
            'person',
            $person1->getId(),
            $person1->setAttribute('library', 'library4')
        );

        // Relate existing document to new document as nested data
        static::getDatabase()->updateDocument(
            'person',
            $person1->getId(),
            $person1->setAttribute('library', $library4)
        );

        // Rename relationship key
        static::getDatabase()->updateRelationship(
            collection: 'person',
            id: 'library',
            newKey: 'newLibrary'
        );

        // Get document with again
        $person = static::getDatabase()->getDocument('person', 'person1');
        $library = $person->getAttribute('newLibrary');
        $this->assertEquals('library4', $library['$id']);

        // Create person with no relationship
        static::getDatabase()->createDocument('person', new Document([
            '$id' => 'person4',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Person 4',
        ]));

        // Can delete parent document with no relation with on delete set to restrict
        $deleted = static::getDatabase()->deleteDocument('person', 'person4');
        $this->assertEquals(true, $deleted);

        $person4 = static::getDatabase()->getDocument('person', 'person4');
        $this->assertEquals(true, $person4->isEmpty());

        // Cannot delete document while still related to another with on delete set to restrict
        try {
            static::getDatabase()->deleteDocument('person', 'person1');
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Cannot delete document because it has at least one related document.', $e->getMessage());
        }

        // Can delete child document while still related to another with on delete set to restrict
        $person5 = static::getDatabase()->createDocument('person', new Document([
            '$id' => 'person5',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Person 5',
            'newLibrary' => [
                '$id' => 'library5',
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'name' => 'Library 5',
                'area' => 'Area 5',
            ],
        ]));
        $deleted = static::getDatabase()->deleteDocument('library', 'library5');
        $this->assertEquals(true, $deleted);
        $person5 = static::getDatabase()->getDocument('person', 'person5');
        $this->assertEquals(null, $person5->getAttribute('newLibrary'));

        // Change on delete to set null
        static::getDatabase()->updateRelationship(
            collection: 'person',
            id: 'newLibrary',
            onDelete: Database::RELATION_MUTATE_SET_NULL
        );

        // Delete parent, no effect on children for one-way
        static::getDatabase()->deleteDocument('person', 'person1');

        // Delete child, set parent relating attribute to null for one-way
        static::getDatabase()->deleteDocument('library', 'library2');

        // Check relation was set to null
        $person2 = static::getDatabase()->getDocument('person', 'person2');
        $this->assertEquals(null, $person2->getAttribute('newLibrary', ''));

        // Relate to another document
        static::getDatabase()->updateDocument(
            'person',
            $person2->getId(),
            $person2->setAttribute('newLibrary', 'library4')
        );

        // Change on delete to cascade
        static::getDatabase()->updateRelationship(
            collection: 'person',
            id: 'newLibrary',
            onDelete: Database::RELATION_MUTATE_CASCADE
        );

        // Delete parent, will delete child
        static::getDatabase()->deleteDocument('person', 'person2');

        // Check parent and child were deleted
        $person = static::getDatabase()->getDocument('person', 'person2');
        $this->assertEquals(true, $person->isEmpty());

        $library = static::getDatabase()->getDocument('library', 'library4');
        $this->assertEquals(true, $library->isEmpty());

        // Delete relationship
        static::getDatabase()->deleteRelationship(
            'person',
            'newLibrary'
        );

        // Check parent doesn't have relationship anymore
        $person = static::getDatabase()->getDocument('person', 'person1');
        $library = $person->getAttribute('newLibrary', '');
        $this->assertEquals(null, $library);
    }

    /**
     * @throws AuthorizationException
     * @throws LimitException
     * @throws DuplicateException
     * @throws StructureException
     * @throws \Throwable
     */
    public function testOneToOneTwoWayRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('country');
        static::getDatabase()->createCollection('city');

        static::getDatabase()->createAttribute('country', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('city', 'code', Database::VAR_STRING, 3, true);
        static::getDatabase()->createAttribute('city', 'name', Database::VAR_STRING, 255, true);

        static::getDatabase()->createRelationship(
            collection: 'country',
            relatedCollection: 'city',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true
        );

        $collection = static::getDatabase()->getCollection('country');
        $attributes = $collection->getAttribute('attributes', []);
        foreach ($attributes as $attribute) {
            if ($attribute['key'] === 'city') {
                $this->assertEquals('relationship', $attribute['type']);
                $this->assertEquals('city', $attribute['$id']);
                $this->assertEquals('city', $attribute['key']);
                $this->assertEquals('city', $attribute['options']['relatedCollection']);
                $this->assertEquals(Database::RELATION_ONE_TO_ONE, $attribute['options']['relationType']);
                $this->assertEquals(true, $attribute['options']['twoWay']);
                $this->assertEquals('country', $attribute['options']['twoWayKey']);
            }
        }

        $collection = static::getDatabase()->getCollection('city');
        $attributes = $collection->getAttribute('attributes', []);
        foreach ($attributes as $attribute) {
            if ($attribute['key'] === 'country') {
                $this->assertEquals('relationship', $attribute['type']);
                $this->assertEquals('country', $attribute['$id']);
                $this->assertEquals('country', $attribute['key']);
                $this->assertEquals('country', $attribute['options']['relatedCollection']);
                $this->assertEquals(Database::RELATION_ONE_TO_ONE, $attribute['options']['relationType']);
                $this->assertEquals(true, $attribute['options']['twoWay']);
                $this->assertEquals('city', $attribute['options']['twoWayKey']);
            }
        }

        // Create document with relationship with nested data
        $doc = new Document([
            '$id' => 'country1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'England',
            'city' => [
                '$id' => 'city1',
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'name' => 'London',
                'code' => 'LON',
            ],
        ]);

        static::getDatabase()->createDocument('country', new Document($doc->getArrayCopy()));
        $country1 = static::getDatabase()->getDocument('country', 'country1');
        $this->assertEquals('London', $country1->getAttribute('city')->getAttribute('name'));

        // Update a document with non existing related document. It should not get added to the list.
        static::getDatabase()->updateDocument('country', 'country1', (new Document($doc->getArrayCopy()))->setAttribute('city', 'no-city'));

        $country1Document = static::getDatabase()->getDocument('country', 'country1');
        // Assert document does not contain non existing relation document.
        $this->assertEquals(null, $country1Document->getAttribute('city'));
        static::getDatabase()->updateDocument('country', 'country1', (new Document($doc->getArrayCopy()))->setAttribute('city', 'city1'));
        try {
            static::getDatabase()->deleteDocument('country', 'country1');
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertInstanceOf(RestrictedException::class, $e);
        }

        $this->assertTrue(static::getDatabase()->deleteDocument('city', 'city1'));

        $city1 = static::getDatabase()->getDocument('city', 'city1');
        $this->assertTrue($city1->isEmpty());

        $country1 = static::getDatabase()->getDocument('country', 'country1');
        $this->assertTrue($country1->getAttribute('city')->isEmpty());

        $this->assertTrue(static::getDatabase()->deleteDocument('country', 'country1'));

        static::getDatabase()->createDocument('country', new Document($doc->getArrayCopy()));
        $country1 = static::getDatabase()->getDocument('country', 'country1');
        $this->assertEquals('London', $country1->getAttribute('city')->getAttribute('name'));

        // Create document with relationship with related ID
        static::getDatabase()->createDocument('city', new Document([
            '$id' => 'city2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Paris',
            'code' => 'PAR',
        ]));
        static::getDatabase()->createDocument('country', new Document([
            '$id' => 'country2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'France',
            'city' => 'city2',
        ]));

        // Create from child side
        static::getDatabase()->createDocument('city', new Document([
            '$id' => 'city3',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Christchurch',
            'code' => 'CHC',
            'country' => [
                '$id' => 'country3',
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'name' => 'New Zealand',
            ],
        ]));
        static::getDatabase()->createDocument('country', new Document([
            '$id' => 'country4',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Australia',
        ]));
        static::getDatabase()->createDocument('city', new Document([
            '$id' => 'city4',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Sydney',
            'code' => 'SYD',
            'country' => 'country4',
        ]));

        // Get document with relationship
        $city = static::getDatabase()->getDocument('city', 'city1');
        $country = $city->getAttribute('country');
        $this->assertEquals('country1', $country['$id']);
        $this->assertArrayNotHasKey('city', $country);

        $city = static::getDatabase()->getDocument('city', 'city2');
        $country = $city->getAttribute('country');
        $this->assertEquals('country2', $country['$id']);
        $this->assertArrayNotHasKey('city', $country);

        $city = static::getDatabase()->getDocument('city', 'city3');
        $country = $city->getAttribute('country');
        $this->assertEquals('country3', $country['$id']);
        $this->assertArrayNotHasKey('city', $country);

        $city = static::getDatabase()->getDocument('city', 'city4');
        $country = $city->getAttribute('country');
        $this->assertEquals('country4', $country['$id']);
        $this->assertArrayNotHasKey('city', $country);

        // Get inverse document with relationship
        $country = static::getDatabase()->getDocument('country', 'country1');
        $city = $country->getAttribute('city');
        $this->assertEquals('city1', $city['$id']);
        $this->assertArrayNotHasKey('country', $city);

        $country = static::getDatabase()->getDocument('country', 'country2');
        $city = $country->getAttribute('city');
        $this->assertEquals('city2', $city['$id']);
        $this->assertArrayNotHasKey('country', $city);

        $country = static::getDatabase()->getDocument('country', 'country3');
        $city = $country->getAttribute('city');
        $this->assertEquals('city3', $city['$id']);
        $this->assertArrayNotHasKey('country', $city);

        $country = static::getDatabase()->getDocument('country', 'country4');
        $city = $country->getAttribute('city');
        $this->assertEquals('city4', $city['$id']);
        $this->assertArrayNotHasKey('country', $city);

        $countries = static::getDatabase()->find('country');

        $this->assertEquals(4, \count($countries));

        // Select related document attributes
        $country = static::getDatabase()->findOne('country', [
            Query::select(['*', 'city.name'])
        ]);

        if ($country->isEmpty()) {
            throw new Exception('Country not found');
        }

        $this->assertEquals('London', $country->getAttribute('city')->getAttribute('name'));
        $this->assertArrayNotHasKey('code', $country->getAttribute('city'));

        $country = static::getDatabase()->getDocument('country', 'country1', [
            Query::select(['*', 'city.name'])
        ]);

        $this->assertEquals('London', $country->getAttribute('city')->getAttribute('name'));
        $this->assertArrayNotHasKey('code', $country->getAttribute('city'));

        $country1 = static::getDatabase()->getDocument('country', 'country1');

        // Update root document attribute without altering relationship
        $country1 = static::getDatabase()->updateDocument(
            'country',
            $country1->getId(),
            $country1->setAttribute('name', 'Country 1 Updated')
        );

        $this->assertEquals('Country 1 Updated', $country1->getAttribute('name'));
        $country1 = static::getDatabase()->getDocument('country', 'country1');
        $this->assertEquals('Country 1 Updated', $country1->getAttribute('name'));

        $city2 = static::getDatabase()->getDocument('city', 'city2');

        // Update inverse root document attribute without altering relationship
        $city2 = static::getDatabase()->updateDocument(
            'city',
            $city2->getId(),
            $city2->setAttribute('name', 'City 2 Updated')
        );

        $this->assertEquals('City 2 Updated', $city2->getAttribute('name'));
        $city2 = static::getDatabase()->getDocument('city', 'city2');
        $this->assertEquals('City 2 Updated', $city2->getAttribute('name'));

        // Update nested document attribute
        $country1 = static::getDatabase()->updateDocument(
            'country',
            $country1->getId(),
            $country1->setAttribute(
                'city',
                $country1
                    ->getAttribute('city')
                    ->setAttribute('name', 'City 1 Updated')
            )
        );

        $this->assertEquals('City 1 Updated', $country1->getAttribute('city')->getAttribute('name'));
        $country1 = static::getDatabase()->getDocument('country', 'country1');
        $this->assertEquals('City 1 Updated', $country1->getAttribute('city')->getAttribute('name'));

        // Update inverse nested document attribute
        $city2 = static::getDatabase()->updateDocument(
            'city',
            $city2->getId(),
            $city2->setAttribute(
                'country',
                $city2
                    ->getAttribute('country')
                    ->setAttribute('name', 'Country 2 Updated')
            )
        );

        $this->assertEquals('Country 2 Updated', $city2->getAttribute('country')->getAttribute('name'));
        $city2 = static::getDatabase()->getDocument('city', 'city2');
        $this->assertEquals('Country 2 Updated', $city2->getAttribute('country')->getAttribute('name'));

        // Create new document with no relationship
        $country5 = static::getDatabase()->createDocument('country', new Document([
            '$id' => 'country5',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Country 5',
        ]));

        // Update to relate to created document
        $country5 = static::getDatabase()->updateDocument(
            'country',
            $country5->getId(),
            $country5->setAttribute('city', new Document([
                '$id' => 'city5',
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                ],
                'name' => 'City 5',
                'code' => 'C5',
            ]))
        );

        $this->assertEquals('city5', $country5->getAttribute('city')['$id']);
        $country5 = static::getDatabase()->getDocument('country', 'country5');
        $this->assertEquals('city5', $country5->getAttribute('city')['$id']);

        // Create new document with no relationship
        $city6 = static::getDatabase()->createDocument('city', new Document([
            '$id' => 'city6',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'City6',
            'code' => 'C6',
        ]));

        // Update to relate to created document
        $city6 = static::getDatabase()->updateDocument(
            'city',
            $city6->getId(),
            $city6->setAttribute('country', new Document([
                '$id' => 'country6',
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                ],
                'name' => 'Country 6',
            ]))
        );

        $this->assertEquals('country6', $city6->getAttribute('country')['$id']);
        $city6 = static::getDatabase()->getDocument('city', 'city6');
        $this->assertEquals('country6', $city6->getAttribute('country')['$id']);

        // One to one can't relate to multiple documents, unique index throws duplicate
        try {
            static::getDatabase()->updateDocument(
                'country',
                $country1->getId(),
                $country1->setAttribute('city', 'city2')
            );
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertInstanceOf(DuplicateException::class, $e);
        }

        $city1 = static::getDatabase()->getDocument('city', 'city1');

        // Set relationship to null
        $city1 = static::getDatabase()->updateDocument(
            'city',
            $city1->getId(),
            $city1->setAttribute('country', null)
        );

        $this->assertEquals(null, $city1->getAttribute('country'));
        $city1 = static::getDatabase()->getDocument('city', 'city1');
        $this->assertEquals(null, $city1->getAttribute('country'));

        // Create a new city with no relation
        $city7 = static::getDatabase()->createDocument('city', new Document([
            '$id' => 'city7',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Copenhagen',
            'code' => 'CPH',
        ]));

        // Update document with relation to new document
        static::getDatabase()->updateDocument(
            'country',
            $country1->getId(),
            $country1->setAttribute('city', 'city7')
        );

        // Relate existing document to new document as nested data
        static::getDatabase()->updateDocument(
            'country',
            $country1->getId(),
            $country1->setAttribute('city', $city7)
        );

        // Create a new country with no relation
        static::getDatabase()->createDocument('country', new Document([
            '$id' => 'country7',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Denmark'
        ]));

        // Update inverse document with new related document
        static::getDatabase()->updateDocument(
            'city',
            $city1->getId(),
            $city1->setAttribute('country', 'country7')
        );

        // Rename relationship keys on both sides
        static::getDatabase()->updateRelationship(
            'country',
            'city',
            'newCity',
            'newCountry'
        );

        // Get document with new relationship key
        $city = static::getDatabase()->getDocument('city', 'city1');
        $country = $city->getAttribute('newCountry');
        $this->assertEquals('country7', $country['$id']);

        // Get inverse document with new relationship key
        $country = static::getDatabase()->getDocument('country', 'country7');
        $city = $country->getAttribute('newCity');
        $this->assertEquals('city1', $city['$id']);

        // Create a new country with no relation
        static::getDatabase()->createDocument('country', new Document([
            '$id' => 'country8',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Denmark'
        ]));

        // Can delete parent document with no relation with on delete set to restrict
        $deleted = static::getDatabase()->deleteDocument('country', 'country8');
        $this->assertEquals(1, $deleted);

        $country8 = static::getDatabase()->getDocument('country', 'country8');
        $this->assertEquals(true, $country8->isEmpty());


        // Cannot delete document while still related to another with on delete set to restrict
        try {
            static::getDatabase()->deleteDocument('country', 'country1');
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Cannot delete document because it has at least one related document.', $e->getMessage());
        }

        // Change on delete to set null
        static::getDatabase()->updateRelationship(
            collection: 'country',
            id: 'newCity',
            onDelete: Database::RELATION_MUTATE_SET_NULL
        );

        static::getDatabase()->updateDocument('city', 'city1', new Document(['newCountry' => null, '$id' => 'city1']));
        $city1 = static::getDatabase()->getDocument('city', 'city1');
        $this->assertNull($city1->getAttribute('newCountry'));

        // Check Delete TwoWay TRUE && RELATION_MUTATE_SET_NULL && related value NULL
        $this->assertTrue(static::getDatabase()->deleteDocument('city', 'city1'));
        $city1 = static::getDatabase()->getDocument('city', 'city1');
        $this->assertTrue($city1->isEmpty());

        // Delete parent, will set child relationship to null for two-way
        static::getDatabase()->deleteDocument('country', 'country1');

        // Check relation was set to null
        $city7 = static::getDatabase()->getDocument('city', 'city7');
        $this->assertEquals(null, $city7->getAttribute('country', ''));

        // Delete child, set parent relationship to null for two-way
        static::getDatabase()->deleteDocument('city', 'city2');

        // Check relation was set to null
        $country2 = static::getDatabase()->getDocument('country', 'country2');
        $this->assertEquals(null, $country2->getAttribute('city', ''));

        // Relate again
        static::getDatabase()->updateDocument(
            'city',
            $city7->getId(),
            $city7->setAttribute('newCountry', 'country2')
        );

        // Change on delete to cascade
        static::getDatabase()->updateRelationship(
            collection: 'country',
            id: 'newCity',
            onDelete: Database::RELATION_MUTATE_CASCADE
        );

        // Delete parent, will delete child
        static::getDatabase()->deleteDocument('country', 'country7');

        // Check parent and child were deleted
        $library = static::getDatabase()->getDocument('country', 'country7');
        $this->assertEquals(true, $library->isEmpty());

        $library = static::getDatabase()->getDocument('city', 'city1');
        $this->assertEquals(true, $library->isEmpty());

        // Delete child, will delete parent for two-way
        static::getDatabase()->deleteDocument('city', 'city7');

        // Check parent and child were deleted
        $library = static::getDatabase()->getDocument('city', 'city7');
        $this->assertEquals(true, $library->isEmpty());

        $library = static::getDatabase()->getDocument('country', 'country2');
        $this->assertEquals(true, $library->isEmpty());

        // Create new document to check after deleting relationship
        static::getDatabase()->createDocument('city', new Document([
            '$id' => 'city7',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Munich',
            'code' => 'MUC',
            'newCountry' => [
                '$id' => 'country7',
                'name' => 'Germany'
            ]
        ]));

        // Delete relationship
        static::getDatabase()->deleteRelationship(
            'country',
            'newCity'
        );

        // Try to get document again
        $country = static::getDatabase()->getDocument('country', 'country4');
        $city = $country->getAttribute('newCity');
        $this->assertEquals(null, $city);

        // Try to get inverse document again
        $city = static::getDatabase()->getDocument('city', 'city7');
        $country = $city->getAttribute('newCountry');
        $this->assertEquals(null, $country);
    }

    public function testIdenticalTwoWayKeyRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('parent');
        static::getDatabase()->createCollection('child');

        static::getDatabase()->createRelationship(
            collection: 'parent',
            relatedCollection: 'child',
            type: Database::RELATION_ONE_TO_ONE,
            id: 'child1'
        );

        try {
            static::getDatabase()->createRelationship(
                collection: 'parent',
                relatedCollection: 'child',
                type: Database::RELATION_ONE_TO_MANY,
                id: 'children',
            );
            $this->fail('Failed to throw Exception');
        } catch (Exception $e) {
            $this->assertEquals('Related attribute already exists', $e->getMessage());
        }

        static::getDatabase()->createRelationship(
            collection: 'parent',
            relatedCollection: 'child',
            type: Database::RELATION_ONE_TO_MANY,
            id: 'children',
            twoWayKey: 'parent_id'
        );

        $collection = static::getDatabase()->getCollection('parent');
        $attributes = $collection->getAttribute('attributes', []);
        foreach ($attributes as $attribute) {
            if ($attribute['key'] === 'child1') {
                $this->assertEquals('parent', $attribute['options']['twoWayKey']);
            }

            if ($attribute['key'] === 'children') {
                $this->assertEquals('parent_id', $attribute['options']['twoWayKey']);
            }
        }

        static::getDatabase()->createDocument('parent', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'child1' => [
                '$id' => 'foo',
                '$permissions' => [Permission::read(Role::any())],
            ],
            'children' => [
                [
                    '$id' => 'bar',
                    '$permissions' => [Permission::read(Role::any())],
                ],
            ],
        ]));

        $documents = static::getDatabase()->find('parent', []);
        $document  = array_pop($documents);
        $this->assertArrayHasKey('child1', $document);
        $this->assertEquals('foo', $document->getAttribute('child1')->getId());
        $this->assertArrayHasKey('children', $document);
        $this->assertEquals('bar', $document->getAttribute('children')[0]->getId());

        try {
            static::getDatabase()->updateRelationship(
                collection: 'parent',
                id: 'children',
                newKey: 'child1'
            );
            $this->fail('Failed to throw Exception');
        } catch (Exception $e) {
            $this->assertEquals('Relationship already exists', $e->getMessage());
        }

        try {
            static::getDatabase()->updateRelationship(
                collection: 'parent',
                id: 'children',
                newTwoWayKey: 'parent'
            );
            $this->fail('Failed to throw Exception');
        } catch (Exception $e) {
            $this->assertEquals('Related attribute already exists', $e->getMessage());
        }
    }

    public function testNestedOneToOne_OneToOneRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('pattern');
        static::getDatabase()->createCollection('shirt');
        static::getDatabase()->createCollection('team');

        static::getDatabase()->createAttribute('pattern', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('shirt', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('team', 'name', Database::VAR_STRING, 255, true);

        static::getDatabase()->createRelationship(
            collection: 'pattern',
            relatedCollection: 'shirt',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
            id: 'shirt',
            twoWayKey: 'pattern'
        );
        static::getDatabase()->createRelationship(
            collection: 'shirt',
            relatedCollection: 'team',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
            id: 'team',
            twoWayKey: 'shirt'
        );

        static::getDatabase()->createDocument('pattern', new Document([
            '$id' => 'stripes',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Stripes',
            'shirt' => [
                '$id' => 'red',
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
                'name' => 'Red',
                'team' => [
                    '$id' => 'reds',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Reds',
                ],
            ],
        ]));

        $pattern = static::getDatabase()->getDocument('pattern', 'stripes');
        $this->assertEquals('red', $pattern['shirt']['$id']);
        $this->assertArrayNotHasKey('pattern', $pattern['shirt']);
        $this->assertEquals('reds', $pattern['shirt']['team']['$id']);
        $this->assertArrayNotHasKey('shirt', $pattern['shirt']['team']);

        static::getDatabase()->createDocument('team', new Document([
            '$id' => 'blues',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Blues',
            'shirt' => [
                '$id' => 'blue',
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
                'name' => 'Blue',
                'pattern' => [
                    '$id' => 'plain',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Plain',
                ],
            ],
        ]));

        $team = static::getDatabase()->getDocument('team', 'blues');
        $this->assertEquals('blue', $team['shirt']['$id']);
        $this->assertArrayNotHasKey('team', $team['shirt']);
        $this->assertEquals('plain', $team['shirt']['pattern']['$id']);
        $this->assertArrayNotHasKey('shirt', $team['shirt']['pattern']);
    }

    public function testNestedOneToOne_OneToManyRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('teachers');
        static::getDatabase()->createCollection('classrooms');
        static::getDatabase()->createCollection('children');

        static::getDatabase()->createAttribute('children', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('teachers', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('classrooms', 'name', Database::VAR_STRING, 255, true);

        static::getDatabase()->createRelationship(
            collection: 'teachers',
            relatedCollection: 'classrooms',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
            id: 'classroom',
            twoWayKey: 'teacher'
        );
        static::getDatabase()->createRelationship(
            collection: 'classrooms',
            relatedCollection: 'children',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            twoWayKey: 'classroom'
        );

        static::getDatabase()->createDocument('teachers', new Document([
            '$id' => 'teacher1',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Teacher 1',
            'classroom' => [
                '$id' => 'classroom1',
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
                'name' => 'Classroom 1',
                'children' => [
                    [
                        '$id' => 'child1',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'name' => 'Child 1',
                    ],
                    [
                        '$id' => 'child2',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'name' => 'Child 2',
                    ],
                ],
            ],
        ]));

        $teacher1 = static::getDatabase()->getDocument('teachers', 'teacher1');
        $this->assertEquals('classroom1', $teacher1['classroom']['$id']);
        $this->assertArrayNotHasKey('teacher', $teacher1['classroom']);
        $this->assertEquals(2, \count($teacher1['classroom']['children']));
        $this->assertEquals('Child 1', $teacher1['classroom']['children'][0]['name']);
        $this->assertEquals('Child 2', $teacher1['classroom']['children'][1]['name']);

        static::getDatabase()->createDocument('children', new Document([
            '$id' => 'child3',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Child 3',
            'classroom' => [
                '$id' => 'classroom2',
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
                'name' => 'Classroom 2',
                'teacher' => [
                    '$id' => 'teacher2',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Teacher 2',
                ],
            ],
        ]));

        $child3 = static::getDatabase()->getDocument('children', 'child3');
        $this->assertEquals('classroom2', $child3['classroom']['$id']);
        $this->assertArrayNotHasKey('children', $child3['classroom']);
        $this->assertEquals('teacher2', $child3['classroom']['teacher']['$id']);
        $this->assertArrayNotHasKey('classroom', $child3['classroom']['teacher']);
    }

    public function testNestedOneToOne_ManyToOneRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('users');
        static::getDatabase()->createCollection('profiles');
        static::getDatabase()->createCollection('avatars');

        static::getDatabase()->createAttribute('users', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('profiles', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('avatars', 'name', Database::VAR_STRING, 255, true);

        static::getDatabase()->createRelationship(
            collection: 'users',
            relatedCollection: 'profiles',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
            id: 'profile',
            twoWayKey: 'user'
        );
        static::getDatabase()->createRelationship(
            collection: 'profiles',
            relatedCollection: 'avatars',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true,
            id: 'avatar',
        );

        static::getDatabase()->createDocument('users', new Document([
            '$id' => 'user1',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'User 1',
            'profile' => [
                '$id' => 'profile1',
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
                'name' => 'Profile 1',
                'avatar' => [
                    '$id' => 'avatar1',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Avatar 1',
                ],
            ],
        ]));

        $user1 = static::getDatabase()->getDocument('users', 'user1');
        $this->assertEquals('profile1', $user1['profile']['$id']);
        $this->assertArrayNotHasKey('user', $user1['profile']);
        $this->assertEquals('avatar1', $user1['profile']['avatar']['$id']);
        $this->assertArrayNotHasKey('profile', $user1['profile']['avatar']);

        static::getDatabase()->createDocument('avatars', new Document([
            '$id' => 'avatar2',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Avatar 2',
            'profiles' => [
                [
                    '$id' => 'profile2',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Profile 2',
                    'user' => [
                        '$id' => 'user2',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'name' => 'User 2',
                    ],
                ]
            ],
        ]));

        $avatar2 = static::getDatabase()->getDocument('avatars', 'avatar2');
        $this->assertEquals('profile2', $avatar2['profiles'][0]['$id']);
        $this->assertArrayNotHasKey('avatars', $avatar2['profiles'][0]);
        $this->assertEquals('user2', $avatar2['profiles'][0]['user']['$id']);
        $this->assertArrayNotHasKey('profiles', $avatar2['profiles'][0]['user']);
    }

    public function testNestedOneToOne_ManyToManyRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('addresses');
        static::getDatabase()->createCollection('houses');
        static::getDatabase()->createCollection('buildings');

        static::getDatabase()->createAttribute('addresses', 'street', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('houses', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('buildings', 'name', Database::VAR_STRING, 255, true);

        static::getDatabase()->createRelationship(
            collection: 'addresses',
            relatedCollection: 'houses',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
            id: 'house',
            twoWayKey: 'address'
        );
        static::getDatabase()->createRelationship(
            collection: 'houses',
            relatedCollection: 'buildings',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
        );

        static::getDatabase()->createDocument('addresses', new Document([
            '$id' => 'address1',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'street' => 'Street 1',
            'house' => [
                '$id' => 'house1',
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
                'name' => 'House 1',
                'buildings' => [
                    [
                        '$id' => 'building1',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'name' => 'Building 1',
                    ],
                    [
                        '$id' => 'building2',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'name' => 'Building 2',
                    ],
                ],
            ],
        ]));

        $address1 = static::getDatabase()->getDocument('addresses', 'address1');
        $this->assertEquals('house1', $address1['house']['$id']);
        $this->assertArrayNotHasKey('address', $address1['house']);
        $this->assertEquals('building1', $address1['house']['buildings'][0]['$id']);
        $this->assertEquals('building2', $address1['house']['buildings'][1]['$id']);
        $this->assertArrayNotHasKey('houses', $address1['house']['buildings'][0]);
        $this->assertArrayNotHasKey('houses', $address1['house']['buildings'][1]);

        static::getDatabase()->createDocument('buildings', new Document([
            '$id' => 'building3',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Building 3',
            'houses' => [
                [
                    '$id' => 'house2',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'House 2',
                    'address' => [
                        '$id' => 'address2',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'street' => 'Street 2',
                    ],
                ],
            ],
        ]));
    }

    public function testExceedMaxDepthOneToOne(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $level1Collection = 'level1OneToOne';
        $level2Collection = 'level2OneToOne';
        $level3Collection = 'level3OneToOne';
        $level4Collection = 'level4OneToOne';

        static::getDatabase()->createCollection($level1Collection);
        static::getDatabase()->createCollection($level2Collection);
        static::getDatabase()->createCollection($level3Collection);
        static::getDatabase()->createCollection($level4Collection);

        static::getDatabase()->createRelationship(
            collection: $level1Collection,
            relatedCollection: $level2Collection,
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
        );
        static::getDatabase()->createRelationship(
            collection: $level2Collection,
            relatedCollection: $level3Collection,
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
        );
        static::getDatabase()->createRelationship(
            collection: $level3Collection,
            relatedCollection: $level4Collection,
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
        );

        // Exceed create depth
        $level1 = static::getDatabase()->createDocument($level1Collection, new Document([
            '$id' => 'level1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            $level2Collection => [
                '$id' => 'level2',
                $level3Collection => [
                    '$id' => 'level3',
                    $level4Collection => [
                        '$id' => 'level4',
                    ],
                ],
            ],
        ]));
        $this->assertArrayHasKey($level2Collection, $level1);
        $this->assertEquals('level2', $level1[$level2Collection]->getId());
        $this->assertArrayHasKey($level3Collection, $level1[$level2Collection]);
        $this->assertEquals('level3', $level1[$level2Collection][$level3Collection]->getId());
        $this->assertArrayNotHasKey($level4Collection, $level1[$level2Collection][$level3Collection]);

        // Confirm the 4th level document does not exist
        $level3 = static::getDatabase()->getDocument($level3Collection, 'level3');
        $this->assertNull($level3[$level4Collection]);

        // Create level 4 document
        $level3->setAttribute($level4Collection, new Document([
            '$id' => 'level4',
        ]));
        $level3 = static::getDatabase()->updateDocument($level3Collection, $level3->getId(), $level3);
        $this->assertEquals('level4', $level3[$level4Collection]->getId());

        // Exceed fetch depth
        $level1 = static::getDatabase()->getDocument($level1Collection, 'level1');
        $this->assertArrayHasKey($level2Collection, $level1);
        $this->assertEquals('level2', $level1[$level2Collection]->getId());
        $this->assertArrayHasKey($level3Collection, $level1[$level2Collection]);
        $this->assertEquals('level3', $level1[$level2Collection][$level3Collection]->getId());
        $this->assertArrayNotHasKey($level4Collection, $level1[$level2Collection][$level3Collection]);
    }

    public function testExceedMaxDepthOneToOneNull(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $level1Collection = 'level1OneToOneNull';
        $level2Collection = 'level2OneToOneNull';
        $level3Collection = 'level3OneToOneNull';
        $level4Collection = 'level4OneToOneNull';

        static::getDatabase()->createCollection($level1Collection);
        static::getDatabase()->createCollection($level2Collection);
        static::getDatabase()->createCollection($level3Collection);
        static::getDatabase()->createCollection($level4Collection);

        static::getDatabase()->createRelationship(
            collection: $level1Collection,
            relatedCollection: $level2Collection,
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
        );
        static::getDatabase()->createRelationship(
            collection: $level2Collection,
            relatedCollection: $level3Collection,
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
        );
        static::getDatabase()->createRelationship(
            collection: $level3Collection,
            relatedCollection: $level4Collection,
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
        );

        $level1 = static::getDatabase()->createDocument($level1Collection, new Document([
            '$id' => 'level1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            $level2Collection => [
                '$id' => 'level2',
                $level3Collection => [
                    '$id' => 'level3',
                    $level4Collection => [
                        '$id' => 'level4',
                    ],
                ],
            ],
        ]));
        $this->assertArrayHasKey($level2Collection, $level1);
        $this->assertEquals('level2', $level1[$level2Collection]->getId());
        $this->assertArrayHasKey($level3Collection, $level1[$level2Collection]);
        $this->assertEquals('level3', $level1[$level2Collection][$level3Collection]->getId());
        $this->assertArrayNotHasKey($level4Collection, $level1[$level2Collection][$level3Collection]);

        // Confirm the 4th level document does not exist
        $level3 = static::getDatabase()->getDocument($level3Collection, 'level3');
        $this->assertNull($level3[$level4Collection]);

        // Create level 4 document
        $level3->setAttribute($level4Collection, new Document([
            '$id' => 'level4',
        ]));
        $level3 = static::getDatabase()->updateDocument($level3Collection, $level3->getId(), $level3);
        $this->assertEquals('level4', $level3[$level4Collection]->getId());
        $level3 = static::getDatabase()->getDocument($level3Collection, 'level3');
        $this->assertEquals('level4', $level3[$level4Collection]->getId());

        // Exceed fetch depth
        $level1 = static::getDatabase()->getDocument($level1Collection, 'level1');
        $this->assertArrayHasKey($level2Collection, $level1);
        $this->assertEquals('level2', $level1[$level2Collection]->getId());
        $this->assertArrayHasKey($level3Collection, $level1[$level2Collection]);
        $this->assertEquals('level3', $level1[$level2Collection][$level3Collection]->getId());
        $this->assertArrayNotHasKey($level4Collection, $level1[$level2Collection][$level3Collection]);
    }

    public function testOneToOneRelationshipKeyWithSymbols(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('$symbols_coll.ection1');
        static::getDatabase()->createCollection('$symbols_coll.ection2');

        static::getDatabase()->createRelationship(
            collection: '$symbols_coll.ection1',
            relatedCollection: '$symbols_coll.ection2',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
        );

        $doc1 = static::getDatabase()->createDocument('$symbols_coll.ection2', new Document([
            '$id' => ID::unique(),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any())
            ]
        ]));
        $doc2 = static::getDatabase()->createDocument('$symbols_coll.ection1', new Document([
            '$id' => ID::unique(),
            '$symbols_coll.ection2' => $doc1->getId(),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any())
            ]
        ]));

        $doc1 = static::getDatabase()->getDocument('$symbols_coll.ection2', $doc1->getId());
        $doc2 = static::getDatabase()->getDocument('$symbols_coll.ection1', $doc2->getId());

        $this->assertEquals($doc2->getId(), $doc1->getAttribute('$symbols_coll.ection1')->getId());
        $this->assertEquals($doc1->getId(), $doc2->getAttribute('$symbols_coll.ection2')->getId());
    }

    public function testRecreateOneToOneOneWayRelationshipFromChild(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }
        static::getDatabase()->createCollection('one', [
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
        static::getDatabase()->createCollection('two', [
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

        static::getDatabase()->createRelationship(
            collection: 'one',
            relatedCollection: 'two',
            type: Database::RELATION_ONE_TO_ONE,
        );

        static::getDatabase()->deleteRelationship('two', 'one');

        $result = static::getDatabase()->createRelationship(
            collection: 'one',
            relatedCollection: 'two',
            type: Database::RELATION_ONE_TO_ONE,
        );

        $this->assertTrue($result);

        static::getDatabase()->deleteCollection('one');
        static::getDatabase()->deleteCollection('two');
    }

    public function testRecreateOneToOneTwoWayRelationshipFromParent(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }
        static::getDatabase()->createCollection('one', [
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
        static::getDatabase()->createCollection('two', [
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

        static::getDatabase()->createRelationship(
            collection: 'one',
            relatedCollection: 'two',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
        );

        static::getDatabase()->deleteRelationship('one', 'two');

        $result = static::getDatabase()->createRelationship(
            collection: 'one',
            relatedCollection: 'two',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
        );

        $this->assertTrue($result);

        static::getDatabase()->deleteCollection('one');
        static::getDatabase()->deleteCollection('two');
    }

    public function testRecreateOneToOneTwoWayRelationshipFromChild(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }
        static::getDatabase()->createCollection('one', [
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
        static::getDatabase()->createCollection('two', [
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

        static::getDatabase()->createRelationship(
            collection: 'one',
            relatedCollection: 'two',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
        );

        static::getDatabase()->deleteRelationship('two', 'one');

        $result = static::getDatabase()->createRelationship(
            collection: 'one',
            relatedCollection: 'two',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
        );

        $this->assertTrue($result);

        static::getDatabase()->deleteCollection('one');
        static::getDatabase()->deleteCollection('two');
    }

    public function testRecreateOneToOneOneWayRelationshipFromParent(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }
        static::getDatabase()->createCollection('one', [
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
        static::getDatabase()->createCollection('two', [
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

        static::getDatabase()->createRelationship(
            collection: 'one',
            relatedCollection: 'two',
            type: Database::RELATION_ONE_TO_ONE,
        );

        static::getDatabase()->deleteRelationship('one', 'two');

        $result = static::getDatabase()->createRelationship(
            collection: 'one',
            relatedCollection: 'two',
            type: Database::RELATION_ONE_TO_ONE,
        );

        $this->assertTrue($result);

        static::getDatabase()->deleteCollection('one');
        static::getDatabase()->deleteCollection('two');
    }

    public function testDeleteBulkDocumentsOneToOneRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships() || !static::getDatabase()->getAdapter()->getSupportForBatchOperations()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->getDatabase()->createCollection('bulk_delete_person_o2o');
        $this->getDatabase()->createCollection('bulk_delete_library_o2o');

        $this->getDatabase()->createAttribute('bulk_delete_person_o2o', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('bulk_delete_library_o2o', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('bulk_delete_library_o2o', 'area', Database::VAR_STRING, 255, true);

        // Restrict
        $this->getDatabase()->createRelationship(
            collection: 'bulk_delete_person_o2o',
            relatedCollection: 'bulk_delete_library_o2o',
            type: Database::RELATION_ONE_TO_ONE,
            onDelete: Database::RELATION_MUTATE_RESTRICT
        );

        $person1 = $this->getDatabase()->createDocument('bulk_delete_person_o2o', new Document([
            '$id' => 'person1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Person 1',
            'bulk_delete_library_o2o' => [
                '$id' => 'library1',
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'name' => 'Library 1',
                'area' => 'Area 1',
            ],
        ]));

        $person1 = $this->getDatabase()->getDocument('bulk_delete_person_o2o', 'person1');
        $library = $person1->getAttribute('bulk_delete_library_o2o');
        $this->assertEquals('library1', $library['$id']);
        $this->assertArrayNotHasKey('bulk_delete_person_o2o', $library);

        // Delete person
        try {
            $this->getDatabase()->deleteDocuments('bulk_delete_person_o2o');
            $this->fail('Failed to throw exception');
        } catch (RestrictedException $e) {
            $this->assertEquals('Cannot delete document because it has at least one related document.', $e->getMessage());
        }

        $this->getDatabase()->updateDocument('bulk_delete_person_o2o', 'person1', new Document([
            '$id' => 'person1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Person 1',
            'bulk_delete_library_o2o' => null,
        ]));

        $this->getDatabase()->deleteDocuments('bulk_delete_person_o2o');
        $this->assertCount(0, $this->getDatabase()->find('bulk_delete_person_o2o'));
        $this->getDatabase()->deleteDocuments('bulk_delete_library_o2o');
        $this->assertCount(0, $this->getDatabase()->find('bulk_delete_library_o2o'));

        // NULL
        $this->getDatabase()->updateRelationship(
            collection: 'bulk_delete_person_o2o',
            id: 'bulk_delete_library_o2o',
            onDelete: Database::RELATION_MUTATE_SET_NULL
        );

        $person1 = $this->getDatabase()->createDocument('bulk_delete_person_o2o', new Document([
            '$id' => 'person1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Person 1',
            'bulk_delete_library_o2o' => [
                '$id' => 'library1',
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'name' => 'Library 1',
                'area' => 'Area 1',
            ],
        ]));

        $person1 = $this->getDatabase()->getDocument('bulk_delete_person_o2o', 'person1');
        $library = $person1->getAttribute('bulk_delete_library_o2o');
        $this->assertEquals('library1', $library['$id']);
        $this->assertArrayNotHasKey('bulk_delete_person_o2o', $library);

        $person = $this->getDatabase()->getDocument('bulk_delete_person_o2o', 'person1');

        $this->getDatabase()->deleteDocuments('bulk_delete_library_o2o');
        $this->assertCount(0, $this->getDatabase()->find('bulk_delete_library_o2o'));
        $this->assertCount(1, $this->getDatabase()->find('bulk_delete_person_o2o'));

        $person = $this->getDatabase()->getDocument('bulk_delete_person_o2o', 'person1');
        $library = $person->getAttribute('bulk_delete_library_o2o');
        $this->assertNull($library);

        // NULL - Cleanup
        $this->getDatabase()->deleteDocuments('bulk_delete_person_o2o');
        $this->assertCount(0, $this->getDatabase()->find('bulk_delete_person_o2o'));
        $this->getDatabase()->deleteDocuments('bulk_delete_library_o2o');
        $this->assertCount(0, $this->getDatabase()->find('bulk_delete_library_o2o'));

        // Cascade
        $this->getDatabase()->updateRelationship(
            collection: 'bulk_delete_person_o2o',
            id: 'bulk_delete_library_o2o',
            onDelete: Database::RELATION_MUTATE_CASCADE
        );

        $person1 = $this->getDatabase()->createDocument('bulk_delete_person_o2o', new Document([
            '$id' => 'person1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Person 1',
            'bulk_delete_library_o2o' => [
                '$id' => 'library1',
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'name' => 'Library 1',
                'area' => 'Area 1',
            ],
        ]));

        $person1 = $this->getDatabase()->getDocument('bulk_delete_person_o2o', 'person1');
        $library = $person1->getAttribute('bulk_delete_library_o2o');
        $this->assertEquals('library1', $library['$id']);
        $this->assertArrayNotHasKey('bulk_delete_person_o2o', $library);

        $person = $this->getDatabase()->getDocument('bulk_delete_person_o2o', 'person1');

        $this->getDatabase()->deleteDocuments('bulk_delete_library_o2o');
        $this->assertCount(0, $this->getDatabase()->find('bulk_delete_library_o2o'));
        $this->assertCount(1, $this->getDatabase()->find('bulk_delete_person_o2o'));

        $person = $this->getDatabase()->getDocument('bulk_delete_person_o2o', 'person1');
        $library = $person->getAttribute('bulk_delete_library_o2o');
        $this->assertEmpty($library);
        $this->assertNotNull($library);

        // Test Bulk delete parent
        $this->getDatabase()->deleteDocuments('bulk_delete_person_o2o');
        $this->assertCount(0, $this->getDatabase()->find('bulk_delete_person_o2o'));

        $person1 = $this->getDatabase()->createDocument('bulk_delete_person_o2o', new Document([
            '$id' => 'person1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Person 1',
            'bulk_delete_library_o2o' => [
                '$id' => 'library1',
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'name' => 'Library 1',
                'area' => 'Area 1',
            ],
        ]));

        $person1 = $this->getDatabase()->getDocument('bulk_delete_person_o2o', 'person1');
        $library = $person1->getAttribute('bulk_delete_library_o2o');
        $this->assertEquals('library1', $library['$id']);
        $this->assertArrayNotHasKey('bulk_delete_person_o2o', $library);

        $this->getDatabase()->deleteDocuments('bulk_delete_person_o2o');
        $this->assertCount(0, $this->getDatabase()->find('bulk_delete_person_o2o'));
        $this->assertCount(0, $this->getDatabase()->find('bulk_delete_library_o2o'));
    }

    public function testDeleteTwoWayRelationshipFromChild(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('drivers');
        static::getDatabase()->createCollection('licenses');

        static::getDatabase()->createRelationship(
            collection: 'drivers',
            relatedCollection: 'licenses',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
            id: 'license',
            twoWayKey: 'driver'
        );

        $drivers = static::getDatabase()->getCollection('drivers');
        $licenses = static::getDatabase()->getCollection('licenses');

        $this->assertEquals(1, \count($drivers->getAttribute('attributes')));
        $this->assertEquals(1, \count($drivers->getAttribute('indexes')));
        $this->assertEquals(1, \count($licenses->getAttribute('attributes')));
        $this->assertEquals(1, \count($licenses->getAttribute('indexes')));

        static::getDatabase()->deleteRelationship('licenses', 'driver');

        $drivers = static::getDatabase()->getCollection('drivers');
        $licenses = static::getDatabase()->getCollection('licenses');

        $this->assertEquals(0, \count($drivers->getAttribute('attributes')));
        $this->assertEquals(0, \count($drivers->getAttribute('indexes')));
        $this->assertEquals(0, \count($licenses->getAttribute('attributes')));
        $this->assertEquals(0, \count($licenses->getAttribute('indexes')));

        static::getDatabase()->createRelationship(
            collection: 'drivers',
            relatedCollection: 'licenses',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            id: 'licenses',
            twoWayKey: 'driver'
        );

        $drivers = static::getDatabase()->getCollection('drivers');
        $licenses = static::getDatabase()->getCollection('licenses');

        $this->assertEquals(1, \count($drivers->getAttribute('attributes')));
        $this->assertEquals(0, \count($drivers->getAttribute('indexes')));
        $this->assertEquals(1, \count($licenses->getAttribute('attributes')));
        $this->assertEquals(1, \count($licenses->getAttribute('indexes')));

        static::getDatabase()->deleteRelationship('licenses', 'driver');

        $drivers = static::getDatabase()->getCollection('drivers');
        $licenses = static::getDatabase()->getCollection('licenses');

        $this->assertEquals(0, \count($drivers->getAttribute('attributes')));
        $this->assertEquals(0, \count($drivers->getAttribute('indexes')));
        $this->assertEquals(0, \count($licenses->getAttribute('attributes')));
        $this->assertEquals(0, \count($licenses->getAttribute('indexes')));

        static::getDatabase()->createRelationship(
            collection: 'licenses',
            relatedCollection: 'drivers',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true,
            id: 'driver',
            twoWayKey: 'licenses'
        );

        $drivers = static::getDatabase()->getCollection('drivers');
        $licenses = static::getDatabase()->getCollection('licenses');

        $this->assertEquals(1, \count($drivers->getAttribute('attributes')));
        $this->assertEquals(0, \count($drivers->getAttribute('indexes')));
        $this->assertEquals(1, \count($licenses->getAttribute('attributes')));
        $this->assertEquals(1, \count($licenses->getAttribute('indexes')));

        static::getDatabase()->deleteRelationship('drivers', 'licenses');

        $drivers = static::getDatabase()->getCollection('drivers');
        $licenses = static::getDatabase()->getCollection('licenses');

        $this->assertEquals(0, \count($drivers->getAttribute('attributes')));
        $this->assertEquals(0, \count($drivers->getAttribute('indexes')));
        $this->assertEquals(0, \count($licenses->getAttribute('attributes')));
        $this->assertEquals(0, \count($licenses->getAttribute('indexes')));

        static::getDatabase()->createRelationship(
            collection: 'licenses',
            relatedCollection: 'drivers',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
            id: 'drivers',
            twoWayKey: 'licenses'
        );

        $drivers = static::getDatabase()->getCollection('drivers');
        $licenses = static::getDatabase()->getCollection('licenses');
        $junction = static::getDatabase()->getCollection('_' . $licenses->getInternalId() . '_' . $drivers->getInternalId());

        $this->assertEquals(1, \count($drivers->getAttribute('attributes')));
        $this->assertEquals(0, \count($drivers->getAttribute('indexes')));
        $this->assertEquals(1, \count($licenses->getAttribute('attributes')));
        $this->assertEquals(0, \count($licenses->getAttribute('indexes')));
        $this->assertEquals(2, \count($junction->getAttribute('attributes')));
        $this->assertEquals(2, \count($junction->getAttribute('indexes')));

        static::getDatabase()->deleteRelationship('drivers', 'licenses');

        $drivers = static::getDatabase()->getCollection('drivers');
        $licenses = static::getDatabase()->getCollection('licenses');
        $junction = static::getDatabase()->getCollection('_licenses_drivers');

        $this->assertEquals(0, \count($drivers->getAttribute('attributes')));
        $this->assertEquals(0, \count($drivers->getAttribute('indexes')));
        $this->assertEquals(0, \count($licenses->getAttribute('attributes')));
        $this->assertEquals(0, \count($licenses->getAttribute('indexes')));

        $this->assertEquals(true, $junction->isEmpty());
    }
}
