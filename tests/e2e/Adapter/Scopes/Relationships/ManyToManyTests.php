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

trait ManyToManyTests
{
    public function testManyToManyOneWayRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('playlist');
        static::getDatabase()->createCollection('song');

        static::getDatabase()->createAttribute('playlist', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('song', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('song', 'length', Database::VAR_INTEGER, 0, true);

        static::getDatabase()->createRelationship(
            collection: 'playlist',
            relatedCollection: 'song',
            type: Database::RELATION_MANY_TO_MANY,
            id: 'songs'
        );

        // Check metadata for collection
        $collection = static::getDatabase()->getCollection('playlist');
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
        $playlist1 = static::getDatabase()->createDocument('playlist', new Document([
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
        static::getDatabase()->createDocument('song', new Document([
            '$id' => 'song2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Song 2',
            'length' => 140,
        ]));
        static::getDatabase()->createDocument('playlist', new Document([
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
        static::getDatabase()->updateDocument('playlist', 'playlist1', $playlist1->setAttribute('songs', ['song1','no-song']));

        $playlist1Document = static::getDatabase()->getDocument('playlist', 'playlist1');
        // Assert document does not contain non existing relation document.
        $this->assertEquals(1, \count($playlist1Document->getAttribute('songs')));

        $documents = static::getDatabase()->find('playlist', [
            Query::select('name'),
            Query::limit(1)
        ]);

        $this->assertArrayNotHasKey('songs', $documents[0]);

        // Get document with relationship
        $playlist = static::getDatabase()->getDocument('playlist', 'playlist1');
        $songs = $playlist->getAttribute('songs', []);
        $this->assertEquals('song1', $songs[0]['$id']);
        $this->assertArrayNotHasKey('playlist', $songs[0]);

        $playlist = static::getDatabase()->getDocument('playlist', 'playlist2');
        $songs = $playlist->getAttribute('songs', []);
        $this->assertEquals('song2', $songs[0]['$id']);
        $this->assertArrayNotHasKey('playlist', $songs[0]);

        // Get related document
        $library = static::getDatabase()->getDocument('song', 'song1');
        $this->assertArrayNotHasKey('songs', $library);

        $library = static::getDatabase()->getDocument('song', 'song2');
        $this->assertArrayNotHasKey('songs', $library);

        $playlists = static::getDatabase()->find('playlist');

        $this->assertEquals(2, \count($playlists));

        // Select related document attributes
        $playlist = static::getDatabase()->findOne('playlist', [
            Query::select('*'),
            Query::select('songs.name')
        ]);

        if ($playlist->isEmpty()) {
            throw new Exception('Playlist not found');
        }

        $this->assertEquals('Song 1', $playlist->getAttribute('songs')[0]->getAttribute('name'));
        $this->assertArrayNotHasKey('length', $playlist->getAttribute('songs')[0]);

        $playlist = static::getDatabase()->getDocument('playlist', 'playlist1', [
            Query::select('*'),
            Query::select('songs.name')
        ]);

        $this->assertEquals('Song 1', $playlist->getAttribute('songs')[0]->getAttribute('name'));
        $this->assertArrayNotHasKey('length', $playlist->getAttribute('songs')[0]);

        // Update root document attribute without altering relationship
        $playlist1 = static::getDatabase()->updateDocument(
            'playlist',
            $playlist1->getId(),
            $playlist1->setAttribute('name', 'Playlist 1 Updated')
        );

        $this->assertEquals('Playlist 1 Updated', $playlist1->getAttribute('name'));
        $playlist1 = static::getDatabase()->getDocument('playlist', 'playlist1');
        $this->assertEquals('Playlist 1 Updated', $playlist1->getAttribute('name'));

        // Update nested document attribute
        $songs = $playlist1->getAttribute('songs', []);
        $songs[0]->setAttribute('name', 'Song 1 Updated');

        $playlist1 = static::getDatabase()->updateDocument(
            'playlist',
            $playlist1->getId(),
            $playlist1->setAttribute('songs', $songs)
        );

        $this->assertEquals('Song 1 Updated', $playlist1->getAttribute('songs')[0]->getAttribute('name'));
        $playlist1 = static::getDatabase()->getDocument('playlist', 'playlist1');
        $this->assertEquals('Song 1 Updated', $playlist1->getAttribute('songs')[0]->getAttribute('name'));

        // Create new document with no relationship
        $playlist5 = static::getDatabase()->createDocument('playlist', new Document([
            '$id' => 'playlist5',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Playlist 5',
        ]));

        // Update to relate to created document
        $playlist5 = static::getDatabase()->updateDocument(
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
        static::getDatabase()->createDocument('playlist', new Document([
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
        $playlist5 = static::getDatabase()->getDocument('playlist', 'playlist5');
        $this->assertEquals('Song 5', $playlist5->getAttribute('songs')[0]->getAttribute('name'));

        // Update document with new related document
        static::getDatabase()->updateDocument(
            'playlist',
            $playlist1->getId(),
            $playlist1->setAttribute('songs', ['song2'])
        );

        // Rename relationship key
        static::getDatabase()->updateRelationship(
            'playlist',
            'songs',
            'newSongs'
        );

        // Get document with new relationship key
        $playlist = static::getDatabase()->getDocument('playlist', 'playlist1');
        $songs = $playlist->getAttribute('newSongs');
        $this->assertEquals('song2', $songs[0]['$id']);

        // Create new document with no relationship
        static::getDatabase()->createDocument('playlist', new Document([
            '$id' => 'playlist3',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Playlist 3',
        ]));

        // Can delete document with no relationship when on delete is set to restrict
        $deleted = static::getDatabase()->deleteDocument('playlist', 'playlist3');
        $this->assertEquals(true, $deleted);

        $playlist3 = static::getDatabase()->getDocument('playlist', 'playlist3');
        $this->assertEquals(true, $playlist3->isEmpty());

        // Try to delete document while still related to another with on delete: restrict
        try {
            static::getDatabase()->deleteDocument('playlist', 'playlist1');
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Cannot delete document because it has at least one related document.', $e->getMessage());
        }

        // Change on delete to set null
        static::getDatabase()->updateRelationship(
            collection: 'playlist',
            id: 'newSongs',
            onDelete: Database::RELATION_MUTATE_SET_NULL
        );

        $playlist1 = static::getDatabase()->getDocument('playlist', 'playlist1');

        // Reset relationships
        static::getDatabase()->updateDocument(
            'playlist',
            $playlist1->getId(),
            $playlist1->setAttribute('newSongs', ['song1'])
        );

        // Delete child, will delete junction
        static::getDatabase()->deleteDocument('song', 'song1');

        // Check relation was set to null
        $playlist1 = static::getDatabase()->getDocument('playlist', 'playlist1');
        $this->assertEquals(0, \count($playlist1->getAttribute('newSongs')));

        // Change on delete to cascade
        static::getDatabase()->updateRelationship(
            collection: 'playlist',
            id: 'newSongs',
            onDelete: Database::RELATION_MUTATE_CASCADE
        );

        // Delete parent, will delete child
        static::getDatabase()->deleteDocument('playlist', 'playlist2');

        // Check parent and child were deleted
        $library = static::getDatabase()->getDocument('playlist', 'playlist2');
        $this->assertEquals(true, $library->isEmpty());

        $library = static::getDatabase()->getDocument('song', 'song2');
        $this->assertEquals(true, $library->isEmpty());

        // Delete relationship
        static::getDatabase()->deleteRelationship(
            'playlist',
            'newSongs'
        );

        // Try to get document again
        $playlist = static::getDatabase()->getDocument('playlist', 'playlist1');
        $songs = $playlist->getAttribute('newSongs');
        $this->assertEquals(null, $songs);
    }

    public function testManyToManyTwoWayRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('students');
        static::getDatabase()->createCollection('classes');

        static::getDatabase()->createAttribute('students', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('classes', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('classes', 'number', Database::VAR_INTEGER, 0, true);

        static::getDatabase()->createRelationship(
            collection: 'students',
            relatedCollection: 'classes',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
        );

        // Check metadata for collection
        $collection = static::getDatabase()->getCollection('students');
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
        $collection = static::getDatabase()->getCollection('classes');
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
        $student1 = static::getDatabase()->createDocument('students', new Document([
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
        static::getDatabase()->updateDocument('students', 'student1', $student1->setAttribute('classes', ['class1', 'no-class']));

        $student1Document = static::getDatabase()->getDocument('students', 'student1');
        // Assert document does not contain non existing relation document.
        $this->assertEquals(1, \count($student1Document->getAttribute('classes')));

        // Create document with relationship with related ID
        static::getDatabase()->createDocument('classes', new Document([
            '$id' => 'class2',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),

            ],
            'name' => 'Class 2',
            'number' => 2,
        ]));
        static::getDatabase()->createDocument('students', new Document([
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
        static::getDatabase()->createDocument('classes', new Document([
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
        static::getDatabase()->createDocument('students', new Document([
            '$id' => 'student4',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Student 4'
        ]));
        static::getDatabase()->createDocument('classes', new Document([
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
        $student = static::getDatabase()->getDocument('students', 'student1');
        $classes = $student->getAttribute('classes', []);
        $this->assertEquals('class1', $classes[0]['$id']);
        $this->assertArrayNotHasKey('students', $classes[0]);

        $student = static::getDatabase()->getDocument('students', 'student2');
        $classes = $student->getAttribute('classes', []);
        $this->assertEquals('class2', $classes[0]['$id']);
        $this->assertArrayNotHasKey('students', $classes[0]);

        $student = static::getDatabase()->getDocument('students', 'student3');
        $classes = $student->getAttribute('classes', []);
        $this->assertEquals('class3', $classes[0]['$id']);
        $this->assertArrayNotHasKey('students', $classes[0]);

        $student = static::getDatabase()->getDocument('students', 'student4');
        $classes = $student->getAttribute('classes', []);
        $this->assertEquals('class4', $classes[0]['$id']);
        $this->assertArrayNotHasKey('students', $classes[0]);

        // Get related document
        $class = static::getDatabase()->getDocument('classes', 'class1');
        $student = $class->getAttribute('students');
        $this->assertEquals('student1', $student[0]['$id']);
        $this->assertArrayNotHasKey('classes', $student[0]);

        $class = static::getDatabase()->getDocument('classes', 'class2');
        $student = $class->getAttribute('students');
        $this->assertEquals('student2', $student[0]['$id']);
        $this->assertArrayNotHasKey('classes', $student[0]);

        $class = static::getDatabase()->getDocument('classes', 'class3');
        $student = $class->getAttribute('students');
        $this->assertEquals('student3', $student[0]['$id']);
        $this->assertArrayNotHasKey('classes', $student[0]);

        $class = static::getDatabase()->getDocument('classes', 'class4');
        $student = $class->getAttribute('students');
        $this->assertEquals('student4', $student[0]['$id']);
        $this->assertArrayNotHasKey('classes', $student[0]);

        // Select related document attributes
        $student = static::getDatabase()->findOne('students', [
            Query::select('*'),
            Query::select('classes.name')
        ]);

        if ($student->isEmpty()) {
            throw new Exception('Student not found');
        }

        $this->assertEquals('Class 1', $student->getAttribute('classes')[0]->getAttribute('name'));
        $this->assertArrayNotHasKey('number', $student->getAttribute('classes')[0]);

        $student = static::getDatabase()->getDocument('students', 'student1', [
            Query::select('*'),
            Query::select('classes.name')
        ]);

        $this->assertEquals('Class 1', $student->getAttribute('classes')[0]->getAttribute('name'));
        $this->assertArrayNotHasKey('number', $student->getAttribute('classes')[0]);

        // Update root document attribute without altering relationship
        $student1 = static::getDatabase()->updateDocument(
            'students',
            $student1->getId(),
            $student1->setAttribute('name', 'Student 1 Updated')
        );

        $this->assertEquals('Student 1 Updated', $student1->getAttribute('name'));
        $student1 = static::getDatabase()->getDocument('students', 'student1');
        $this->assertEquals('Student 1 Updated', $student1->getAttribute('name'));

        // Update inverse root document attribute without altering relationship
        $class2 = static::getDatabase()->getDocument('classes', 'class2');
        $class2 = static::getDatabase()->updateDocument(
            'classes',
            $class2->getId(),
            $class2->setAttribute('name', 'Class 2 Updated')
        );

        $this->assertEquals('Class 2 Updated', $class2->getAttribute('name'));
        $class2 = static::getDatabase()->getDocument('classes', 'class2');
        $this->assertEquals('Class 2 Updated', $class2->getAttribute('name'));

        // Update nested document attribute
        $classes = $student1->getAttribute('classes', []);
        $classes[0]->setAttribute('name', 'Class 1 Updated');

        $student1 = static::getDatabase()->updateDocument(
            'students',
            $student1->getId(),
            $student1->setAttribute('classes', $classes)
        );

        $this->assertEquals('Class 1 Updated', $student1->getAttribute('classes')[0]->getAttribute('name'));
        $student1 = static::getDatabase()->getDocument('students', 'student1');
        $this->assertEquals('Class 1 Updated', $student1->getAttribute('classes')[0]->getAttribute('name'));

        // Update inverse nested document attribute
        $students = $class2->getAttribute('students', []);
        $students[0]->setAttribute('name', 'Student 2 Updated');

        $class2 = static::getDatabase()->updateDocument(
            'classes',
            $class2->getId(),
            $class2->setAttribute('students', $students)
        );

        $this->assertEquals('Student 2 Updated', $class2->getAttribute('students')[0]->getAttribute('name'));
        $class2 = static::getDatabase()->getDocument('classes', 'class2');
        $this->assertEquals('Student 2 Updated', $class2->getAttribute('students')[0]->getAttribute('name'));

        // Create new document with no relationship
        $student5 = static::getDatabase()->createDocument('students', new Document([
            '$id' => 'student5',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Student 5',
        ]));

        // Update to relate to created document
        $student5 = static::getDatabase()->updateDocument(
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
        $student5 = static::getDatabase()->getDocument('students', 'student5');
        $this->assertEquals('Class 5', $student5->getAttribute('classes')[0]->getAttribute('name'));

        // Create child document with no relationship
        $class6 = static::getDatabase()->createDocument('classes', new Document([
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
        $class6 = static::getDatabase()->updateDocument(
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
        $class6 = static::getDatabase()->getDocument('classes', 'class6');
        $this->assertEquals('Student 6', $class6->getAttribute('students')[0]->getAttribute('name'));

        // Update document with new related document
        static::getDatabase()->updateDocument(
            'students',
            $student1->getId(),
            $student1->setAttribute('classes', ['class2'])
        );

        $class1 = static::getDatabase()->getDocument('classes', 'class1');

        // Update inverse document
        static::getDatabase()->updateDocument(
            'classes',
            $class1->getId(),
            $class1->setAttribute('students', ['student1'])
        );

        // Rename relationship keys on both sides
        static::getDatabase()->updateRelationship(
            'students',
            'classes',
            'newClasses',
            'newStudents'
        );

        // Get document with new relationship key
        $students = static::getDatabase()->getDocument('students', 'student1');
        $classes = $students->getAttribute('newClasses');
        $this->assertEquals('class2', $classes[0]['$id']);

        // Get inverse document with new relationship key
        $class = static::getDatabase()->getDocument('classes', 'class1');
        $students = $class->getAttribute('newStudents');
        $this->assertEquals('student1', $students[0]['$id']);

        // Create new document with no relationship
        static::getDatabase()->createDocument('students', new Document([
            '$id' => 'student7',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Student 7',
        ]));

        // Can delete document with no relationship when on delete is set to restrict
        $deleted = static::getDatabase()->deleteDocument('students', 'student7');
        $this->assertEquals(true, $deleted);

        $student6 = static::getDatabase()->getDocument('students', 'student7');
        $this->assertEquals(true, $student6->isEmpty());

        // Try to delete document while still related to another with on delete: restrict
        try {
            static::getDatabase()->deleteDocument('students', 'student1');
            $this->fail('Failed to throw exception');
        } catch (Exception $e) {
            $this->assertEquals('Cannot delete document because it has at least one related document.', $e->getMessage());
        }

        // Change on delete to set null
        static::getDatabase()->updateRelationship(
            collection: 'students',
            id: 'newClasses',
            onDelete: Database::RELATION_MUTATE_SET_NULL
        );

        $student1 = static::getDatabase()->getDocument('students', 'student1');

        // Reset relationships
        static::getDatabase()->updateDocument(
            'students',
            $student1->getId(),
            $student1->setAttribute('newClasses', ['class1'])
        );

        // Delete child, will delete junction
        static::getDatabase()->deleteDocument('classes', 'class1');

        // Check relation was set to null
        $student1 = static::getDatabase()->getDocument('students', 'student1');
        $this->assertEquals(0, \count($student1->getAttribute('newClasses')));

        // Change on delete to cascade
        static::getDatabase()->updateRelationship(
            collection: 'students',
            id: 'newClasses',
            onDelete: Database::RELATION_MUTATE_CASCADE
        );

        // Delete parent, will delete child
        static::getDatabase()->deleteDocument('students', 'student2');

        // Check parent and child were deleted
        $library = static::getDatabase()->getDocument('students', 'student2');
        $this->assertEquals(true, $library->isEmpty());

        // Delete child, should not delete parent
        static::getDatabase()->deleteDocument('classes', 'class6');

        // Check only child was deleted
        $student6 = static::getDatabase()->getDocument('students', 'student6');
        $this->assertEquals(false, $student6->isEmpty());
        $this->assertEmpty($student6->getAttribute('newClasses'));

        $library = static::getDatabase()->getDocument('classes', 'class2');
        $this->assertEquals(true, $library->isEmpty());

        // Delete relationship
        static::getDatabase()->deleteRelationship(
            'students',
            'newClasses'
        );

        // Try to get documents again
        $student = static::getDatabase()->getDocument('students', 'student1');
        $classes = $student->getAttribute('newClasses');
        $this->assertEquals(null, $classes);

        // Try to get inverse documents again
        $classes = static::getDatabase()->getDocument('classes', 'class1');
        $students = $classes->getAttribute('newStudents');
        $this->assertEquals(null, $students);
    }

    public function testNestedManyToMany_OneToOneRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('stones');
        static::getDatabase()->createCollection('hearths');
        static::getDatabase()->createCollection('plots');

        static::getDatabase()->createAttribute('stones', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('hearths', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('plots', 'name', Database::VAR_STRING, 255, true);

        static::getDatabase()->createRelationship(
            collection: 'stones',
            relatedCollection: 'hearths',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
        );
        static::getDatabase()->createRelationship(
            collection: 'hearths',
            relatedCollection: 'plots',
            type: Database::RELATION_ONE_TO_ONE,
            twoWay: true,
            id: 'plot',
            twoWayKey: 'hearth'
        );

        static::getDatabase()->createDocument('stones', new Document([
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

        $stone1 = static::getDatabase()->getDocument('stones', 'stone1');
        $this->assertEquals(2, \count($stone1['hearths']));
        $this->assertEquals('hearth1', $stone1['hearths'][0]['$id']);
        $this->assertEquals('hearth2', $stone1['hearths'][1]['$id']);
        $this->assertArrayNotHasKey('stone', $stone1['hearths'][0]);
        $this->assertEquals('plot1', $stone1['hearths'][0]['plot']['$id']);
        $this->assertEquals('plot2', $stone1['hearths'][1]['plot']['$id']);
        $this->assertArrayNotHasKey('hearth', $stone1['hearths'][0]['plot']);

        static::getDatabase()->createDocument('plots', new Document([
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

        $plot3 = static::getDatabase()->getDocument('plots', 'plot3');
        $this->assertEquals('hearth3', $plot3['hearth']['$id']);
        $this->assertArrayNotHasKey('plot', $plot3['hearth']);
        $this->assertEquals('stone2', $plot3['hearth']['stones'][0]['$id']);
        $this->assertArrayNotHasKey('hearths', $plot3['hearth']['stones'][0]);
    }

    public function testNestedManyToMany_OneToManyRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('groups');
        static::getDatabase()->createCollection('tounaments');
        static::getDatabase()->createCollection('prizes');

        static::getDatabase()->createAttribute('groups', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('tounaments', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('prizes', 'name', Database::VAR_STRING, 255, true);

        static::getDatabase()->createRelationship(
            collection: 'groups',
            relatedCollection: 'tounaments',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
        );
        static::getDatabase()->createRelationship(
            collection: 'tounaments',
            relatedCollection: 'prizes',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true,
            id: 'prizes',
            twoWayKey: 'tounament'
        );

        static::getDatabase()->createDocument('groups', new Document([
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

        $group1 = static::getDatabase()->getDocument('groups', 'group1');
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
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('platforms');
        static::getDatabase()->createCollection('games');
        static::getDatabase()->createCollection('publishers');

        static::getDatabase()->createAttribute('platforms', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('games', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('publishers', 'name', Database::VAR_STRING, 255, true);

        static::getDatabase()->createRelationship(
            collection: 'platforms',
            relatedCollection: 'games',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
        );
        static::getDatabase()->createRelationship(
            collection: 'games',
            relatedCollection: 'publishers',
            type: Database::RELATION_MANY_TO_ONE,
            twoWay: true,
            id: 'publisher',
            twoWayKey: 'games'
        );

        static::getDatabase()->createDocument('platforms', new Document([
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

        $platform1 = static::getDatabase()->getDocument('platforms', 'platform1');
        $this->assertEquals(2, \count($platform1['games']));
        $this->assertEquals('game1', $platform1['games'][0]['$id']);
        $this->assertEquals('game2', $platform1['games'][1]['$id']);
        $this->assertArrayNotHasKey('platforms', $platform1['games'][0]);
        $this->assertEquals('publisher1', $platform1['games'][0]['publisher']['$id']);
        $this->assertEquals('publisher2', $platform1['games'][1]['publisher']['$id']);
        $this->assertArrayNotHasKey('games', $platform1['games'][0]['publisher']);

        static::getDatabase()->createDocument('publishers', new Document([
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

        $publisher3 = static::getDatabase()->getDocument('publishers', 'publisher3');
        $this->assertEquals(1, \count($publisher3['games']));
        $this->assertEquals('game3', $publisher3['games'][0]['$id']);
        $this->assertArrayNotHasKey('publisher', $publisher3['games'][0]);
        $this->assertEquals('platform2', $publisher3['games'][0]['platforms'][0]['$id']);
        $this->assertArrayNotHasKey('games', $publisher3['games'][0]['platforms'][0]);
    }

    public function testNestedManyToMany_ManyToManyRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('sauces');
        static::getDatabase()->createCollection('pizzas');
        static::getDatabase()->createCollection('toppings');

        static::getDatabase()->createAttribute('sauces', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('pizzas', 'name', Database::VAR_STRING, 255, true);
        static::getDatabase()->createAttribute('toppings', 'name', Database::VAR_STRING, 255, true);

        static::getDatabase()->createRelationship(
            collection: 'sauces',
            relatedCollection: 'pizzas',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
        );
        static::getDatabase()->createRelationship(
            collection: 'pizzas',
            relatedCollection: 'toppings',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
            id: 'toppings',
            twoWayKey: 'pizzas'
        );

        static::getDatabase()->createDocument('sauces', new Document([
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

        $sauce1 = static::getDatabase()->getDocument('sauces', 'sauce1');
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
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        static::getDatabase()->createCollection('$symbols_coll.ection7');
        static::getDatabase()->createCollection('$symbols_coll.ection8');

        static::getDatabase()->createRelationship(
            collection: '$symbols_coll.ection7',
            relatedCollection: '$symbols_coll.ection8',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
        );

        $doc1 = static::getDatabase()->createDocument('$symbols_coll.ection8', new Document([
            '$id' => ID::unique(),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any())
            ]
        ]));
        $doc2 = static::getDatabase()->createDocument('$symbols_coll.ection7', new Document([
            '$id' => ID::unique(),
            '$symbols_coll.ection8' => [$doc1->getId()],
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any())
            ]
        ]));

        $doc1 = static::getDatabase()->getDocument('$symbols_coll.ection8', $doc1->getId());
        $doc2 = static::getDatabase()->getDocument('$symbols_coll.ection7', $doc2->getId());

        $this->assertEquals($doc2->getId(), $doc1->getAttribute('$symbols_coll.ection7')[0]->getId());
        $this->assertEquals($doc1->getId(), $doc2->getAttribute('$symbols_coll.ection8')[0]->getId());
    }

    public function testRecreateManyToManyOneWayRelationshipFromChild(): void
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
            type: Database::RELATION_MANY_TO_MANY,
        );

        static::getDatabase()->deleteRelationship('two', 'one');

        $result = static::getDatabase()->createRelationship(
            collection: 'one',
            relatedCollection: 'two',
            type: Database::RELATION_MANY_TO_MANY,
        );

        $this->assertTrue($result);

        static::getDatabase()->deleteCollection('one');
        static::getDatabase()->deleteCollection('two');
    }

    public function testRecreateManyToManyTwoWayRelationshipFromParent(): void
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
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
        );

        static::getDatabase()->deleteRelationship('one', 'two');

        $result = static::getDatabase()->createRelationship(
            collection: 'one',
            relatedCollection: 'two',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
        );

        $this->assertTrue($result);

        static::getDatabase()->deleteCollection('one');
        static::getDatabase()->deleteCollection('two');
    }

    public function testRecreateManyToManyTwoWayRelationshipFromChild(): void
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
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
        );

        static::getDatabase()->deleteRelationship('two', 'one');

        $result = static::getDatabase()->createRelationship(
            collection: 'one',
            relatedCollection: 'two',
            type: Database::RELATION_MANY_TO_MANY,
            twoWay: true,
        );

        $this->assertTrue($result);

        static::getDatabase()->deleteCollection('one');
        static::getDatabase()->deleteCollection('two');
    }

    public function testRecreateManyToManyOneWayRelationshipFromParent(): void
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
            type: Database::RELATION_MANY_TO_MANY,
        );

        static::getDatabase()->deleteRelationship('one', 'two');

        $result = static::getDatabase()->createRelationship(
            collection: 'one',
            relatedCollection: 'two',
            type: Database::RELATION_MANY_TO_MANY,
        );

        $this->assertTrue($result);

        static::getDatabase()->deleteCollection('one');
        static::getDatabase()->deleteCollection('two');
    }

    public function testDeleteBulkDocumentsManyToManyRelationship(): void
    {
        if (!static::getDatabase()->getAdapter()->getSupportForRelationships() || !static::getDatabase()->getAdapter()->getSupportForBatchOperations()) {
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
}
