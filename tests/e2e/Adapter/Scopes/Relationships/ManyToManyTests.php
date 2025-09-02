<?php

namespace Tests\E2E\Adapter\Scopes\Relationships;

use Exception;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Restricted as RestrictedException;
use Utopia\Database\Exception\Structure;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;

trait ManyToManyTests
{
    public function testManyToManyOneWayRelationship(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $database->createCollection('playlist');
        $database->createCollection('song');

        $database->createAttribute('playlist', 'name', Database::VAR_STRING, 255, true);
        $database->createAttribute('song', 'name', Database::VAR_STRING, 255, true);
        $database->createAttribute('song', 'length', Database::VAR_INTEGER, 0, true);

        $database->createRelationship(
            collection: 'playlist',
            relatedCollection: 'song',
            type: Database::RELATION_MANY_TO_MANY,
            id: 'songs'
        );

        // Check metadata for collection
        $collection = $database->getCollection('playlist');
        $attributes = $collection->getAttribute('attributes', []);

        foreach ($attributes as $attribute) {
            if ($attribute['key'] === 'songs') {
                $this->assertEquals('relationship', $attribute['type']);
                $this->assertEquals('songs', $attribute['$id']);
                $this->assertEquals('songs', $attribute['key']);
                $this->assertEquals('song', $attribute['options']['relatedCollection']);
                $this->assertEquals(Database::RELATION_MANY_TO_MANY, $attribute['options']['relationType']);
                $this->assertEquals(false, $attribute['options']['twoWay']);
                $this->assertEquals('playlist', $attribute['options']['twoWayKey']);
            }
        }

        // Create document with relationship with nested data
        $playlist1 = $database->createDocument('playlist', new Document([
            '$id' => 'playlist1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Playlist 1',
            'songs' => [
                [
                    '$id' => 'song1',
                    '$permissions' => [
                        Permission::read(Role::any()),
                        Permission::update(Role::any()),
                        Permission::delete(Role::any()),
                    ],
                    'name' => 'Song 1',
                    'length' => 180,
                ],
            ],
        ]));

        // Create document with relationship with related ID
        $database->createDocument('song', new Document([
            '$id' => 'song2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Song 2',
            'length' => 140,
        ]));
        $database->createDocument('playlist', new Document([
            '$id' => 'playlist2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Playlist 2',
            'songs' => [
                'song2'
            ]
        ]));

        // Update a document with non existing related document. It should not get added to the list.
        $database->updateDocument('playlist', 'playlist1', $playlist1->setAttribute('songs', ['song1','no-song']));

        $playlist1Document = $database->getDocument('playlist', 'playlist1');
        // Assert document does not contain non existing relation document.
        $this->assertEquals(1, \count($playlist1Document->getAttribute('songs')));

        $documents = static::getDatabase()->find('playlist', [
            Query::select('name'),
            Query::limit(1)
        ]);

        $this->assertArrayNotHasKey('songs', $documents[0]);

        // Get document with relationship
        $playlist = $database->getDocument('playlist', 'playlist1');
        $songs = $playlist->getAttribute('songs', []);
        $this->assertEquals('song1', $songs[0]['$id']);
        $this->assertArrayNotHasKey('playlist', $songs[0]);

        $playlist = $database->getDocument('playlist', 'playlist2');
        $songs = $playlist->getAttribute('songs', []);
        $this->assertEquals('song2', $songs[0]['$id']);
        $this->assertArrayNotHasKey('playlist', $songs[0]);

        // Get related document
        $library = $database->getDocument('song', 'song1');
        $this->assertArrayNotHasKey('songs', $library);

        $library = $database->getDocument('song', 'song2');
        $this->assertArrayNotHasKey('songs', $library);

        $playlists = $database->find('playlist');

        $this->assertEquals(2, \count($playlists));

        // Select related document attributes
        $playlist = $database->findOne('playlist', [
            Query::select('*'),
            Query::select('songs.name')
        ]);

        if ($playlist->isEmpty()) {
            throw new Exception('Playlist not found');
        }

        $this->assertEquals('Song 1', $playlist->getAttribute('songs')[0]->getAttribute('name'));
        $this->assertArrayNotHasKey('length', $playlist->getAttribute('songs')[0]);

        $playlist = $database->getDocument('playlist', 'playlist1', [
            Query::select('*'),
            Query::select('songs.name')
        ]);

        $this->assertEquals('Song 1', $playlist->getAttribute('songs')[0]->getAttribute('name'));
        $this->assertArrayNotHasKey('length', $playlist->getAttribute('songs')[0]);

        // Update root document attribute without altering relationship
        $playlist1 = $database->updateDocument(
            'playlist',
            $playlist1->getId(),
            $playlist1->setAttribute('name', 'Playlist 1 Updated')
        );

        $this->assertEquals('Playlist 1 Updated', $playlist1->getAttribute('name'));
        $playlist1 = $database->getDocument('playlist', 'playlist1');
        $this->assertEquals('Playlist 1 Updated', $playlist1->getAttribute('name'));

        // Update nested document attribute
        $songs = $playlist1->getAttribute('songs', []);
        $songs[0]->setAttribute('name', 'Song 1 Updated');

        $playlist1 = $database->updateDocument(
            'playlist',
            $playlist1->getId(),
            $playlist1->setAttribute('songs', $songs)
        );

        $this->assertEquals('Song 1 Updated', $playlist1->getAttribute('songs')[0]->getAttribute('name'));
        $playlist1 = $database->getDocument('playlist', 'playlist1');
        $this->assertEquals('Song 1 Updated', $playlist1->getAttribute('songs')[0]->getAttribute('name'));

        // Create new document with no relationship
        $playlist5 = $database->createDocument('playlist', new Document([
            '$id' => 'playlist5',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Playlist 5',
        ]));

        // Update to relate to created document
        $playlist5 = $database->updateDocument(
            'playlist',
            $playlist5->getId(),
            $playlist5->setAttribute('songs', [new Document([
                '$id' => 'song5',
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'name' => 'Song 5',
                'length' => 180,
            ])])
        );

        // Playlist relating to existing songs that belong to other playlists
        $database->createDocument('playlist', new Document([
            '$id' => 'playlist6',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Playlist 6',
            'songs' => [
                'song1',
                'song2',
                'song5'
            ]
        ]));

        $this->assertEquals('Song 5', $playlist5->getAttribute('songs')[0]->getAttribute('name'));
        $playlist5 = $database->getDocument('playlist', 'playlist5');
        $this->assertEquals('Song 5', $playlist5->getAttribute('songs')[0]->getAttribute('name'));

        // Update document with new related document
        $database->updateDocument(
            'playlist',
            $playlist1->getId(),
            $playlist1->setAttribute('songs', ['song2'])
        );

        // Rename relationship key
        $database->updateRelationship(
            'playlist',
            'songs',
            'newSongs'
        );

        // Get document with new relationship key
        $playlist = $database->getDocument('playlist', 'playlist1');
        $songs = $playlist->getAttribute('newSongs');
        $this->assertEquals('song2', $songs[0]['$id']);

        // Create new document with no relationship
        $database->createDocument('playlist', new Document([
            '$id' => 'playlist3',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Playlist 3',
        ]));

        // Can delete document with no relationship when on delete is set to restrict
        $deleted = $database->deleteDocument('playlist', 'playlist3');
        $this->assertEquals(true, $deleted);

        $playlist3 = $database->getDocument('playlist', 'playlist3');
        $this->assertEquals(true, $playlist3->isEmpty());

        // Try to delete document while still related to another with on delete: restrict
        try {
            $database->deleteDocument('playlist', 'playlist1');
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Cannot delete document because it has at least one related document.', $e->getMessage());
        }

        // Change on delete to set null
        $database->updateRelationship(
            collection: 'playlist',
            id: 'newSongs',
            onDelete: Database::RELATION_MUTATE_SET_NULL
        );

        $playlist1 = $database->getDocument('playlist', 'playlist1');

        // Reset relationships
        $database->updateDocument(
            'playlist',
            $playlist1->getId(),
            $playlist1->setAttribute('newSongs', ['song1'])
        );

        // Delete child, will delete junction
        $database->deleteDocument('song', 'song1');

        // Check relation was set to null
        $playlist1 = $database->getDocument('playlist', 'playlist1');
        $this->assertEquals(0, \count($playlist1->getAttribute('newSongs')));

        // Change on delete to cascade
        $database->updateRelationship(
            collection: 'playlist',
            id: 'newSongs',
            onDelete: Database::RELATION_MUTATE_CASCADE
        );

        // Delete parent, will delete child
        $database->deleteDocument('playlist', 'playlist2');

        // Check parent and child were deleted
        $library = $database->getDocument('playlist', 'playlist2');
        $this->assertEquals(true, $library->isEmpty());

        $library = $database->getDocument('song', 'song2');
        $this->assertEquals(true, $library->isEmpty());

        // Delete relationship
        $database->deleteRelationship(
            'playlist',
            'newSongs'
        );

        // Try to get document again
        $playlist = $database->getDocument('playlist', 'playlist1');
        $songs = $playlist->getAttribute('newSongs');
        $this->assertEquals(null, $songs);
    }

    public function testManyToManyTwoWayRelationship(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $database->createCollection('students');
        $database->createCollection('classes');

        $database->createAttribute('students', 'name', Database::VAR_STRING, 255, true);
        $database->createAttribute('classes', 'name', Database::VAR_STRING, 255, true);
        $database->createAttribute('classes', 'number', Database::VAR_INTEGER, 0, true);

        $database->createRelationship(
            collection: 'students',
            relatedCollection: 'classes',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
        );

        // Check metadata for collection
        $collection = $database->getCollection('students');
        $attributes = $collection->getAttribute('attributes', []);
        foreach ($attributes as $attribute) {
            if ($attribute['key'] === 'students') {
                $this->assertEquals('relationship', $attribute['type']);
                $this->assertEquals('students', $attribute['$id']);
                $this->assertEquals('students', $attribute['key']);
                $this->assertEquals('students', $attribute['options']['relatedCollection']);
                $this->assertEquals(Database::RELATION_MANY_TO_MANY, $attribute['options']['relationType']);
                $this->assertEquals(true, $attribute['options']['twoWay']);
                $this->assertEquals('classes', $attribute['options']['twoWayKey']);
            }
        }

        // Check metadata for related collection
        $collection = $database->getCollection('classes');
        $attributes = $collection->getAttribute('attributes', []);
        foreach ($attributes as $attribute) {
            if ($attribute['key'] === 'classes') {
                $this->assertEquals('relationship', $attribute['type']);
                $this->assertEquals('classes', $attribute['$id']);
                $this->assertEquals('classes', $attribute['key']);
                $this->assertEquals('classes', $attribute['options']['relatedCollection']);
                $this->assertEquals(Database::RELATION_MANY_TO_MANY, $attribute['options']['relationType']);
                $this->assertEquals(true, $attribute['options']['twoWay']);
                $this->assertEquals('students', $attribute['options']['twoWayKey']);
            }
        }

        // Create document with relationship with nested data
        $student1 = $database->createDocument('students', new Document([
            '$id' => 'student1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Student 1',
            'classes' => [
                [
                    '$id' => 'class1',
                    '$permissions' => [
                        Permission::read(Role::any()),
                        Permission::update(Role::any()),
                        Permission::delete(Role::any()),
                    ],
                    'name' => 'Class 1',
                    'number' => 1,
                ],
            ],
        ]));

        // Update a document with non existing related document. It should not get added to the list.
        $database->updateDocument('students', 'student1', $student1->setAttribute('classes', ['class1', 'no-class']));

        $student1Document = $database->getDocument('students', 'student1');
        // Assert document does not contain non existing relation document.
        $this->assertEquals(1, \count($student1Document->getAttribute('classes')));

        // Create document with relationship with related ID
        $database->createDocument('classes', new Document([
            '$id' => 'class2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),

            ],
            'name' => 'Class 2',
            'number' => 2,
        ]));
        $database->createDocument('students', new Document([
            '$id' => 'student2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Student 2',
            'classes' => [
                'class2'
            ],
        ]));

        // Create from child side
        $database->createDocument('classes', new Document([
            '$id' => 'class3',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Class 3',
            'number' => 3,
            'students' => [
                [
                    '$id' => 'student3',
                    '$permissions' => [
                        Permission::read(Role::any()),
                        Permission::update(Role::any()),
                        Permission::delete(Role::any()),
                    ],
                    'name' => 'Student 3',
                ]
            ],
        ]));
        $database->createDocument('students', new Document([
            '$id' => 'student4',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Student 4'
        ]));
        $database->createDocument('classes', new Document([
            '$id' => 'class4',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),

            ],
            'name' => 'Class 4',
            'number' => 4,
            'students' => [
                'student4'
            ],
        ]));

        // Get document with relationship
        $student = $database->getDocument('students', 'student1');
        $classes = $student->getAttribute('classes', []);
        $this->assertEquals('class1', $classes[0]['$id']);
        $this->assertArrayNotHasKey('students', $classes[0]);

        $student = $database->getDocument('students', 'student2');
        $classes = $student->getAttribute('classes', []);
        $this->assertEquals('class2', $classes[0]['$id']);
        $this->assertArrayNotHasKey('students', $classes[0]);

        $student = $database->getDocument('students', 'student3');
        $classes = $student->getAttribute('classes', []);
        $this->assertEquals('class3', $classes[0]['$id']);
        $this->assertArrayNotHasKey('students', $classes[0]);

        $student = $database->getDocument('students', 'student4');
        $classes = $student->getAttribute('classes', []);
        $this->assertEquals('class4', $classes[0]['$id']);
        $this->assertArrayNotHasKey('students', $classes[0]);

        // Get related document
        $class = $database->getDocument('classes', 'class1');
        $student = $class->getAttribute('students');
        $this->assertEquals('student1', $student[0]['$id']);
        $this->assertArrayNotHasKey('classes', $student[0]);

        $class = $database->getDocument('classes', 'class2');
        $student = $class->getAttribute('students');
        $this->assertEquals('student2', $student[0]['$id']);
        $this->assertArrayNotHasKey('classes', $student[0]);

        $class = $database->getDocument('classes', 'class3');
        $student = $class->getAttribute('students');
        $this->assertEquals('student3', $student[0]['$id']);
        $this->assertArrayNotHasKey('classes', $student[0]);

        $class = $database->getDocument('classes', 'class4');
        $student = $class->getAttribute('students');
        $this->assertEquals('student4', $student[0]['$id']);
        $this->assertArrayNotHasKey('classes', $student[0]);

        // Select related document attributes
        $student = $database->findOne('students', [
            Query::select('*'),
            Query::select('classes.name')
        ]);

        if ($student->isEmpty()) {
            throw new Exception('Student not found');
        }

        $this->assertEquals('Class 1', $student->getAttribute('classes')[0]->getAttribute('name'));
        $this->assertArrayNotHasKey('number', $student->getAttribute('classes')[0]);

        $student = $database->getDocument('students', 'student1', [
            Query::select('*'),
            Query::select('classes.name')
        ]);

        $this->assertEquals('Class 1', $student->getAttribute('classes')[0]->getAttribute('name'));
        $this->assertArrayNotHasKey('number', $student->getAttribute('classes')[0]);

        // Update root document attribute without altering relationship
        $student1 = $database->updateDocument(
            'students',
            $student1->getId(),
            $student1->setAttribute('name', 'Student 1 Updated')
        );

        $this->assertEquals('Student 1 Updated', $student1->getAttribute('name'));
        $student1 = $database->getDocument('students', 'student1');
        $this->assertEquals('Student 1 Updated', $student1->getAttribute('name'));

        // Update inverse root document attribute without altering relationship
        $class2 = $database->getDocument('classes', 'class2');
        $class2 = $database->updateDocument(
            'classes',
            $class2->getId(),
            $class2->setAttribute('name', 'Class 2 Updated')
        );

        $this->assertEquals('Class 2 Updated', $class2->getAttribute('name'));
        $class2 = $database->getDocument('classes', 'class2');
        $this->assertEquals('Class 2 Updated', $class2->getAttribute('name'));

        // Update nested document attribute
        $classes = $student1->getAttribute('classes', []);
        $classes[0]->setAttribute('name', 'Class 1 Updated');

        $student1 = $database->updateDocument(
            'students',
            $student1->getId(),
            $student1->setAttribute('classes', $classes)
        );

        $this->assertEquals('Class 1 Updated', $student1->getAttribute('classes')[0]->getAttribute('name'));
        $student1 = $database->getDocument('students', 'student1');
        $this->assertEquals('Class 1 Updated', $student1->getAttribute('classes')[0]->getAttribute('name'));

        // Update inverse nested document attribute
        $students = $class2->getAttribute('students', []);
        $students[0]->setAttribute('name', 'Student 2 Updated');

        $class2 = $database->updateDocument(
            'classes',
            $class2->getId(),
            $class2->setAttribute('students', $students)
        );

        $this->assertEquals('Student 2 Updated', $class2->getAttribute('students')[0]->getAttribute('name'));
        $class2 = $database->getDocument('classes', 'class2');
        $this->assertEquals('Student 2 Updated', $class2->getAttribute('students')[0]->getAttribute('name'));

        // Create new document with no relationship
        $student5 = $database->createDocument('students', new Document([
            '$id' => 'student5',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Student 5',
        ]));

        // Update to relate to created document
        $student5 = $database->updateDocument(
            'students',
            $student5->getId(),
            $student5->setAttribute('classes', [new Document([
                '$id' => 'class5',
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'name' => 'Class 5',
                'number' => 5,
            ])])
        );

        $this->assertEquals('Class 5', $student5->getAttribute('classes')[0]->getAttribute('name'));
        $student5 = $database->getDocument('students', 'student5');
        $this->assertEquals('Class 5', $student5->getAttribute('classes')[0]->getAttribute('name'));

        // Create child document with no relationship
        $class6 = $database->createDocument('classes', new Document([
            '$id' => 'class6',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Class 6',
            'number' => 6,
        ]));

        // Update to relate to created document
        $class6 = $database->updateDocument(
            'classes',
            $class6->getId(),
            $class6->setAttribute('students', [new Document([
                '$id' => 'student6',
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'name' => 'Student 6',
            ])])
        );

        $this->assertEquals('Student 6', $class6->getAttribute('students')[0]->getAttribute('name'));
        $class6 = $database->getDocument('classes', 'class6');
        $this->assertEquals('Student 6', $class6->getAttribute('students')[0]->getAttribute('name'));

        // Update document with new related document
        $database->updateDocument(
            'students',
            $student1->getId(),
            $student1->setAttribute('classes', ['class2'])
        );

        $class1 = $database->getDocument('classes', 'class1');

        // Update inverse document
        $database->updateDocument(
            'classes',
            $class1->getId(),
            $class1->setAttribute('students', ['student1'])
        );

        // Rename relationship keys on both sides
        $database->updateRelationship(
            'students',
            'classes',
            'newClasses',
            'newStudents'
        );

        // Get document with new relationship key
        $students = $database->getDocument('students', 'student1');
        $classes = $students->getAttribute('newClasses');
        $this->assertEquals('class2', $classes[0]['$id']);

        // Get inverse document with new relationship key
        $class = $database->getDocument('classes', 'class1');
        $students = $class->getAttribute('newStudents');
        $this->assertEquals('student1', $students[0]['$id']);

        // Create new document with no relationship
        $database->createDocument('students', new Document([
            '$id' => 'student7',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Student 7',
        ]));

        // Can delete document with no relationship when on delete is set to restrict
        $deleted = $database->deleteDocument('students', 'student7');
        $this->assertEquals(true, $deleted);

        $student6 = $database->getDocument('students', 'student7');
        $this->assertEquals(true, $student6->isEmpty());

        // Try to delete document while still related to another with on delete: restrict
        try {
            $database->deleteDocument('students', 'student1');
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Cannot delete document because it has at least one related document.', $e->getMessage());
        }

        // Change on delete to set null
        $database->updateRelationship(
            collection: 'students',
            id: 'newClasses',
            onDelete: Database::RELATION_MUTATE_SET_NULL
        );

        $student1 = $database->getDocument('students', 'student1');

        // Reset relationships
        $database->updateDocument(
            'students',
            $student1->getId(),
            $student1->setAttribute('newClasses', ['class1'])
        );

        // Delete child, will delete junction
        $database->deleteDocument('classes', 'class1');

        // Check relation was set to null
        $student1 = $database->getDocument('students', 'student1');
        $this->assertEquals(0, \count($student1->getAttribute('newClasses')));

        // Change on delete to cascade
        $database->updateRelationship(
            collection: 'students',
            id: 'newClasses',
            onDelete: Database::RELATION_MUTATE_CASCADE
        );

        // Delete parent, will delete child
        $database->deleteDocument('students', 'student2');

        // Check parent and child were deleted
        $library = $database->getDocument('students', 'student2');
        $this->assertEquals(true, $library->isEmpty());

        // Delete child, should not delete parent
        $database->deleteDocument('classes', 'class6');

        // Check only child was deleted
        $student6 = $database->getDocument('students', 'student6');
        $this->assertEquals(false, $student6->isEmpty());
        $this->assertEmpty($student6->getAttribute('newClasses'));

        $library = $database->getDocument('classes', 'class2');
        $this->assertEquals(true, $library->isEmpty());

        // Delete relationship
        $database->deleteRelationship(
            'students',
            'newClasses'
        );

        // Try to get documents again
        $student = $database->getDocument('students', 'student1');
        $classes = $student->getAttribute('newClasses');
        $this->assertEquals(null, $classes);

        // Try to get inverse documents again
        $classes = $database->getDocument('classes', 'class1');
        $students = $classes->getAttribute('newStudents');
        $this->assertEquals(null, $students);
    }

    public function testNestedManyToMany_OneToOneRelationship(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $database->createCollection('stones');
        $database->createCollection('hearths');
        $database->createCollection('plots');

        $database->createAttribute('stones', 'name', Database::VAR_STRING, 255, true);
        $database->createAttribute('hearths', 'name', Database::VAR_STRING, 255, true);
        $database->createAttribute('plots', 'name', Database::VAR_STRING, 255, true);

        $database->createRelationship(
            collection: 'stones',
            relatedCollection: 'hearths',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
        );
        $database->createRelationship(
            collection: 'hearths',
            relatedCollection: 'plots',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
            id: 'plot',
            twoWayKey: 'hearth'
        );

        $database->createDocument('stones', new Document([
            '$id' => 'stone1',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Building 1',
            'hearths' => [
                [
                    '$id' => 'hearth1',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'House 1',
                    'plot' => [
                        '$id' => 'plot1',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'name' => 'Address 1',
                    ],
                ],
                [
                    '$id' => 'hearth2',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'House 2',
                    'plot' => [
                        '$id' => 'plot2',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'name' => 'Address 2',
                    ],
                ],
            ],
        ]));

        $stone1 = $database->getDocument('stones', 'stone1');
        $this->assertEquals(2, \count($stone1['hearths']));
        $this->assertEquals('hearth1', $stone1['hearths'][0]['$id']);
        $this->assertEquals('hearth2', $stone1['hearths'][1]['$id']);
        $this->assertArrayNotHasKey('stone', $stone1['hearths'][0]);
        $this->assertEquals('plot1', $stone1['hearths'][0]['plot']['$id']);
        $this->assertEquals('plot2', $stone1['hearths'][1]['plot']['$id']);
        $this->assertArrayNotHasKey('hearth', $stone1['hearths'][0]['plot']);

        $database->createDocument('plots', new Document([
            '$id' => 'plot3',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Address 3',
            'hearth' => [
                '$id' => 'hearth3',
                '$permissions' => [
                    Permission::read(Role::any()),
                ],
                'name' => 'Hearth 3',
                'stones' => [
                    [
                        '$id' => 'stone2',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'name' => 'Stone 2',
                    ],
                ],
            ],
        ]));

        $plot3 = $database->getDocument('plots', 'plot3');
        $this->assertEquals('hearth3', $plot3['hearth']['$id']);
        $this->assertArrayNotHasKey('plot', $plot3['hearth']);
        $this->assertEquals('stone2', $plot3['hearth']['stones'][0]['$id']);
        $this->assertArrayNotHasKey('hearths', $plot3['hearth']['stones'][0]);
    }

    public function testNestedManyToMany_OneToManyRelationship(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $database->createCollection('groups');
        $database->createCollection('tounaments');
        $database->createCollection('prizes');

        $database->createAttribute('groups', 'name', Database::VAR_STRING, 255, true);
        $database->createAttribute('tounaments', 'name', Database::VAR_STRING, 255, true);
        $database->createAttribute('prizes', 'name', Database::VAR_STRING, 255, true);

        $database->createRelationship(
            collection: 'groups',
            relatedCollection: 'tounaments',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
        );
        $database->createRelationship(
            collection: 'tounaments',
            relatedCollection: 'prizes',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            id: 'prizes',
            twoWayKey: 'tounament'
        );

        $database->createDocument('groups', new Document([
            '$id' => 'group1',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Group 1',
            'tounaments' => [
                [
                    '$id' => 'tounament1',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Tounament 1',
                    'prizes' => [
                        [
                            '$id' => 'prize1',
                            '$permissions' => [
                                Permission::read(Role::any()),
                            ],
                            'name' => 'Prize 1',
                        ],
                        [
                            '$id' => 'prize2',
                            '$permissions' => [
                                Permission::read(Role::any()),
                            ],
                            'name' => 'Prize 2',
                        ],
                    ],
                ],
                [
                    '$id' => 'tounament2',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Tounament 2',
                    'prizes' => [
                        [
                            '$id' => 'prize3',
                            '$permissions' => [
                                Permission::read(Role::any()),
                            ],
                            'name' => 'Prize 3',
                        ],
                        [
                            '$id' => 'prize4',
                            '$permissions' => [
                                Permission::read(Role::any()),
                            ],
                            'name' => 'Prize 4',
                        ],
                    ],
                ],
            ],
        ]));

        $group1 = $database->getDocument('groups', 'group1');
        $this->assertEquals(2, \count($group1['tounaments']));
        $this->assertEquals('tounament1', $group1['tounaments'][0]['$id']);
        $this->assertEquals('tounament2', $group1['tounaments'][1]['$id']);
        $this->assertArrayNotHasKey('group', $group1['tounaments'][0]);
        $this->assertEquals(2, \count($group1['tounaments'][0]['prizes']));
        $this->assertEquals('prize1', $group1['tounaments'][0]['prizes'][0]['$id']);
        $this->assertEquals('prize2', $group1['tounaments'][0]['prizes'][1]['$id']);
        $this->assertArrayNotHasKey('tounament', $group1['tounaments'][0]['prizes'][0]);
    }

    public function testNestedManyToMany_ManyToOneRelationship(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $database->createCollection('platforms');
        $database->createCollection('games');
        $database->createCollection('publishers');

        $database->createAttribute('platforms', 'name', Database::VAR_STRING, 255, true);
        $database->createAttribute('games', 'name', Database::VAR_STRING, 255, true);
        $database->createAttribute('publishers', 'name', Database::VAR_STRING, 255, true);

        $database->createRelationship(
            collection: 'platforms',
            relatedCollection: 'games',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
        );
        $database->createRelationship(
            collection: 'games',
            relatedCollection: 'publishers',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true,
            id: 'publisher',
            twoWayKey: 'games'
        );

        $database->createDocument('platforms', new Document([
            '$id' => 'platform1',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Platform 1',
            'games' => [
                [
                    '$id' => 'game1',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Game 1',
                    'publisher' => [
                        '$id' => 'publisher1',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'name' => 'Publisher 1',
                    ],
                ],
                [
                    '$id' => 'game2',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Game 2',
                    'publisher' => [
                        '$id' => 'publisher2',
                        '$permissions' => [
                            Permission::read(Role::any()),
                        ],
                        'name' => 'Publisher 2',
                    ],
                ],
            ]
        ]));

        $platform1 = $database->getDocument('platforms', 'platform1');
        $this->assertEquals(2, \count($platform1['games']));
        $this->assertEquals('game1', $platform1['games'][0]['$id']);
        $this->assertEquals('game2', $platform1['games'][1]['$id']);
        $this->assertArrayNotHasKey('platforms', $platform1['games'][0]);
        $this->assertEquals('publisher1', $platform1['games'][0]['publisher']['$id']);
        $this->assertEquals('publisher2', $platform1['games'][1]['publisher']['$id']);
        $this->assertArrayNotHasKey('games', $platform1['games'][0]['publisher']);

        $database->createDocument('publishers', new Document([
            '$id' => 'publisher3',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Publisher 3',
            'games' => [
                [
                    '$id' => 'game3',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Game 3',
                    'platforms' => [
                        [
                            '$id' => 'platform2',
                            '$permissions' => [
                                Permission::read(Role::any()),
                            ],
                            'name' => 'Platform 2',
                        ]
                    ],
                ],
            ],
        ]));

        $publisher3 = $database->getDocument('publishers', 'publisher3');
        $this->assertEquals(1, \count($publisher3['games']));
        $this->assertEquals('game3', $publisher3['games'][0]['$id']);
        $this->assertArrayNotHasKey('publisher', $publisher3['games'][0]);
        $this->assertEquals('platform2', $publisher3['games'][0]['platforms'][0]['$id']);
        $this->assertArrayNotHasKey('games', $publisher3['games'][0]['platforms'][0]);
    }

    public function testNestedManyToMany_ManyToManyRelationship(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $database->createCollection('sauces');
        $database->createCollection('pizzas');
        $database->createCollection('toppings');

        $database->createAttribute('sauces', 'name', Database::VAR_STRING, 255, true);
        $database->createAttribute('pizzas', 'name', Database::VAR_STRING, 255, true);
        $database->createAttribute('toppings', 'name', Database::VAR_STRING, 255, true);

        $database->createRelationship(
            collection: 'sauces',
            relatedCollection: 'pizzas',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
        );
        $database->createRelationship(
            collection: 'pizzas',
            relatedCollection: 'toppings',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
            id: 'toppings',
            twoWayKey: 'pizzas'
        );

        $database->createDocument('sauces', new Document([
            '$id' => 'sauce1',
            '$permissions' => [
                Permission::read(Role::any()),
            ],
            'name' => 'Sauce 1',
            'pizzas' => [
                [
                    '$id' => 'pizza1',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Pizza 1',
                    'toppings' => [
                        [
                            '$id' => 'topping1',
                            '$permissions' => [
                                Permission::read(Role::any()),
                            ],
                            'name' => 'Topping 1',
                        ],
                        [
                            '$id' => 'topping2',
                            '$permissions' => [
                                Permission::read(Role::any()),
                            ],
                            'name' => 'Topping 2',
                        ],
                    ],
                ],
                [
                    '$id' => 'pizza2',
                    '$permissions' => [
                        Permission::read(Role::any()),
                    ],
                    'name' => 'Pizza 2',
                    'toppings' => [
                        [
                            '$id' => 'topping3',
                            '$permissions' => [
                                Permission::read(Role::any()),
                            ],
                            'name' => 'Topping 3',
                        ],
                        [
                            '$id' => 'topping4',
                            '$permissions' => [
                                Permission::read(Role::any()),
                            ],
                            'name' => 'Topping 4',
                        ],
                    ],
                ],
            ]
        ]));

        $sauce1 = $database->getDocument('sauces', 'sauce1');
        $this->assertEquals(2, \count($sauce1['pizzas']));
        $this->assertEquals('pizza1', $sauce1['pizzas'][0]['$id']);
        $this->assertEquals('pizza2', $sauce1['pizzas'][1]['$id']);
        $this->assertArrayNotHasKey('sauces', $sauce1['pizzas'][0]);
        $this->assertEquals(2, \count($sauce1['pizzas'][0]['toppings']));
        $this->assertEquals('topping1', $sauce1['pizzas'][0]['toppings'][0]['$id']);
        $this->assertEquals('topping2', $sauce1['pizzas'][0]['toppings'][1]['$id']);
        $this->assertArrayNotHasKey('pizzas', $sauce1['pizzas'][0]['toppings'][0]);
        $this->assertEquals(2, \count($sauce1['pizzas'][1]['toppings']));
        $this->assertEquals('topping3', $sauce1['pizzas'][1]['toppings'][0]['$id']);
        $this->assertEquals('topping4', $sauce1['pizzas'][1]['toppings'][1]['$id']);
        $this->assertArrayNotHasKey('pizzas', $sauce1['pizzas'][1]['toppings'][0]);
    }

    public function testManyToManyRelationshipKeyWithSymbols(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $database->createCollection('$symbols_coll.ection7');
        $database->createCollection('$symbols_coll.ection8');

        $database->createRelationship(
            collection: '$symbols_coll.ection7',
            relatedCollection: '$symbols_coll.ection8',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
        );

        $doc1 = $database->createDocument('$symbols_coll.ection8', new Document([
            '$id' => ID::unique(),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any())
            ]
        ]));
        $doc2 = $database->createDocument('$symbols_coll.ection7', new Document([
            '$id' => ID::unique(),
            '$symbols_coll.ection8' => [$doc1->getId()],
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any())
            ]
        ]));

        $doc1 = $database->getDocument('$symbols_coll.ection8', $doc1->getId());
        $doc2 = $database->getDocument('$symbols_coll.ection7', $doc2->getId());

        $this->assertEquals($doc2->getId(), $doc1->getAttribute('$symbols_coll.ection7')[0]->getId());
        $this->assertEquals($doc1->getId(), $doc2->getAttribute('$symbols_coll.ection8')[0]->getId());
    }

    public function testRecreateManyToManyOneWayRelationshipFromChild(): void
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
            type: Database::RELATION_MANY_TO_MANY,
        );

        $database->deleteRelationship('two', 'one');

        $result = $database->createRelationship(
            collection: 'one',
            relatedCollection: 'two',
            type: Database::RELATION_MANY_TO_MANY,
        );

        $this->assertTrue($result);

        $database->deleteCollection('one');
        $database->deleteCollection('two');
    }

    public function testRecreateManyToManyTwoWayRelationshipFromParent(): void
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
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
        );

        $database->deleteRelationship('one', 'two');

        $result = $database->createRelationship(
            collection: 'one',
            relatedCollection: 'two',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
        );

        $this->assertTrue($result);

        $database->deleteCollection('one');
        $database->deleteCollection('two');
    }

    public function testRecreateManyToManyTwoWayRelationshipFromChild(): void
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
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
        );

        $database->deleteRelationship('two', 'one');

        $result = $database->createRelationship(
            collection: 'one',
            relatedCollection: 'two',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
        );

        $this->assertTrue($result);

        $database->deleteCollection('one');
        $database->deleteCollection('two');
    }

    public function testRecreateManyToManyOneWayRelationshipFromParent(): void
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
            type: Database::RELATION_MANY_TO_MANY,
        );

        $database->deleteRelationship('one', 'two');

        $result = $database->createRelationship(
            collection: 'one',
            relatedCollection: 'two',
            type: Database::RELATION_MANY_TO_MANY,
        );

        $this->assertTrue($result);

        $database->deleteCollection('one');
        $database->deleteCollection('two');
    }

    public function testSelectManyToMany(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $database->createCollection('select_m2m_collection1');
        $database->createCollection('select_m2m_collection2');

        $database->createAttribute('select_m2m_collection1', 'name', Database::VAR_STRING, 255, true);
        $database->createAttribute('select_m2m_collection1', 'type', Database::VAR_STRING, 255, true);
        $database->createAttribute('select_m2m_collection2', 'name', Database::VAR_STRING, 255, true);
        $database->createAttribute('select_m2m_collection2', 'type', Database::VAR_STRING, 255, true);

        // Many-to-Many Relationship
        $database->createRelationship(
            collection: 'select_m2m_collection1',
            relatedCollection: 'select_m2m_collection2',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true
        );

        // Create documents in the first collection
        $doc1 = $database->createDocument('select_m2m_collection1', new Document([
            '$id' => 'doc1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Document 1',
            'type' => 'Type A',
            'select_m2m_collection2' => [
                [
                    '$id' => 'related_doc1',
                    '$permissions' => [
                        Permission::read(Role::any()),
                        Permission::update(Role::any()),
                        Permission::delete(Role::any()),
                    ],
                    'name' => 'Related Document 1',
                    'type' => 'Type B',
                ],
                [
                    '$id' => 'related_doc2',
                    '$permissions' => [
                        Permission::read(Role::any()),
                        Permission::update(Role::any()),
                        Permission::delete(Role::any()),
                    ],
                    'name' => 'Related Document 2',
                    'type' => 'Type C',
                ],
            ],
        ]));

        // Use select query to get only name of the related documents
        $docs = $database->find('select_m2m_collection1', [
            Query::select(['name', 'select_m2m_collection2.name']),
        ]);

        $this->assertCount(1, $docs);
        $this->assertEquals('Document 1', $docs[0]->getAttribute('name'));
        $this->assertArrayNotHasKey('type', $docs[0]);

        $relatedDocs = $docs[0]->getAttribute('select_m2m_collection2');

        $this->assertCount(2, $relatedDocs);
        $this->assertEquals('Related Document 1', $relatedDocs[0]->getAttribute('name'));
        $this->assertEquals('Related Document 2', $relatedDocs[1]->getAttribute('name'));
        $this->assertArrayNotHasKey('type', $relatedDocs[0]);
        $this->assertArrayNotHasKey('type', $relatedDocs[1]);
    }

    public function testSelectAcrossMultipleCollections(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        // Create collections
        $database->createCollection('artists', permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ], documentSecurity: false);
        $database->createCollection('albums', permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ], documentSecurity: false);
        $database->createCollection('tracks', permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any())
        ], documentSecurity: false);

        // Add attributes
        $database->createAttribute('artists', 'name', Database::VAR_STRING, 255, true);
        $database->createAttribute('albums', 'name', Database::VAR_STRING, 255, true);
        $database->createAttribute('tracks', 'title', Database::VAR_STRING, 255, true);
        $database->createAttribute('tracks', 'duration', Database::VAR_INTEGER, 0, true);

        // Create relationships
        $database->createRelationship(
            collection: 'artists',
            relatedCollection: 'albums',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true
        );

        $database->createRelationship(
            collection: 'albums',
            relatedCollection: 'tracks',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true
        );

        // Create documents
        $database->createDocument('artists', new Document([
            '$id' => 'artist1',
            'name' => 'The Great Artist',
            'albums' => [
                [
                    '$id' => 'album1',
                    'name' => 'First Album',
                    'tracks' => [
                        [
                            '$id' => 'track1',
                            'title' => 'Hit Song 1',
                            'duration' => 180,
                        ],
                        [
                            '$id' => 'track2',
                            'title' => 'Hit Song 2',
                            'duration' => 220,
                        ]
                    ]
                ],
                [
                    '$id' => 'album2',
                    'name' => 'Second Album',
                    'tracks' => [
                        [
                            '$id' => 'track3',
                            'title' => 'Ballad 3',
                            'duration' => 240,
                        ]
                    ]
                ]
            ]
        ]));

        // Query with nested select
        $artists = $database->find('artists', [
            Query::select(['name', 'albums.name', 'albums.tracks.title'])
        ]);

        $this->assertCount(1, $artists);
        $artist = $artists[0];
        $this->assertEquals('The Great Artist', $artist->getAttribute('name'));
        $this->assertArrayHasKey('albums', $artist->getArrayCopy());

        $albums = $artist->getAttribute('albums');
        $this->assertCount(2, $albums);

        $album1 = $albums[0];
        $this->assertEquals('First Album', $album1->getAttribute('name'));
        $this->assertArrayHasKey('tracks', $album1->getArrayCopy());
        $this->assertArrayNotHasKey('artists', $album1->getArrayCopy());

        $album2 = $albums[1];
        $this->assertEquals('Second Album', $album2->getAttribute('name'));
        $this->assertArrayHasKey('tracks', $album2->getArrayCopy());

        $album1Tracks = $album1->getAttribute('tracks');
        $this->assertCount(2, $album1Tracks);
        $this->assertEquals('Hit Song 1', $album1Tracks[0]->getAttribute('title'));
        $this->assertArrayNotHasKey('duration', $album1Tracks[0]->getArrayCopy());
        $this->assertEquals('Hit Song 2', $album1Tracks[1]->getAttribute('title'));
        $this->assertArrayNotHasKey('duration', $album1Tracks[1]->getArrayCopy());

        $album2Tracks = $album2->getAttribute('tracks');
        $this->assertCount(1, $album2Tracks);
        $this->assertEquals('Ballad 3', $album2Tracks[0]->getAttribute('title'));
        $this->assertArrayNotHasKey('duration', $album2Tracks[0]->getArrayCopy());
    }

    public function testDeleteBulkDocumentsManyToManyRelationship(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForRelationships() || !$database->getAdapter()->getSupportForBatchOperations()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->getDatabase()->createCollection('bulk_delete_person_m2m');
        $this->getDatabase()->createCollection('bulk_delete_library_m2m');

        $this->getDatabase()->createAttribute('bulk_delete_person_m2m', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('bulk_delete_library_m2m', 'name', Database::VAR_STRING, 255, true);
        $this->getDatabase()->createAttribute('bulk_delete_library_m2m', 'area', Database::VAR_STRING, 255, true);

        // Many-to-Many Relationship
        $this->getDatabase()->createRelationship(
            collection: 'bulk_delete_person_m2m',
            relatedCollection: 'bulk_delete_library_m2m',
            type: Database::RELATION_MANY_TO_MANY,
            onDelete: Database::RELATION_MUTATE_RESTRICT
        );

        $person1 = $this->getDatabase()->createDocument('bulk_delete_person_m2m', new Document([
            '$id' => 'person1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Person 1',
            'bulk_delete_library_m2m' => [
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

        $person1 = $this->getDatabase()->getDocument('bulk_delete_person_m2m', 'person1');
        $libraries = $person1->getAttribute('bulk_delete_library_m2m');
        $this->assertCount(2, $libraries);

        // Delete person
        try {
            $this->getDatabase()->deleteDocuments('bulk_delete_person_m2m');
            $this->fail('Failed to throw exception');
        } catch (RestrictedException $e) {
            $this->assertEquals('Cannot delete document because it has at least one related document.', $e->getMessage());
        }

        // Restrict Cleanup
        $this->getDatabase()->deleteRelationship('bulk_delete_person_m2m', 'bulk_delete_library_m2m');
        $this->getDatabase()->deleteDocuments('bulk_delete_library_m2m');
        $this->assertCount(0, $this->getDatabase()->find('bulk_delete_library_m2m'));

        $this->getDatabase()->deleteDocuments('bulk_delete_person_m2m');
        $this->assertCount(0, $this->getDatabase()->find('bulk_delete_person_m2m'));
    }
    public function testUpdateParentAndChild_ManyToMany(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (
            !$database->getAdapter()->getSupportForRelationships() ||
            !$database->getAdapter()->getSupportForBatchOperations()
        ) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $parentCollection = 'parent_combined_m2m';
        $childCollection = 'child_combined_m2m';

        $database->createCollection($parentCollection);
        $database->createCollection($childCollection);

        $database->createAttribute($parentCollection, 'name', Database::VAR_STRING, 255, true);
        $database->createAttribute($childCollection, 'name', Database::VAR_STRING, 255, true);
        $database->createAttribute($childCollection, 'parentNumber', Database::VAR_INTEGER, 0, false);


        $database->createRelationship(
            collection: $parentCollection,
            relatedCollection: $childCollection,
            type: Database::RELATION_MANY_TO_MANY,
            id: 'parentNumber'
        );

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


    public function testDeleteDocumentsRelationshipErrorDoesNotDeleteParent_ManyToMany(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForRelationships() || !$database->getAdapter()->getSupportForBatchOperations()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $parentCollection = 'parent_relationship_many_to_many';
        $childCollection = 'child_relationship_many_to_many';

        $database->createCollection($parentCollection);
        $database->createCollection($childCollection);
        $database->createAttribute($parentCollection, 'name', Database::VAR_STRING, 255, true);
        $database->createAttribute($childCollection, 'name', Database::VAR_STRING, 255, true);

        $database->createRelationship(
            collection: $parentCollection,
            relatedCollection: $childCollection,
            type: Database::RELATION_MANY_TO_MANY,
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
