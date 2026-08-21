<?php

namespace Tests\E2E\Adapter\Scopes\Relationships;

use Exception;
use Utopia\Database\Adapter\Feature;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Collection;
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

trait ManyToManyTests
{
    public function testManyToManyOneWayRelationship(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! ($database->getAdapter()->hasFeature(Feature\Relationships::class))) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection(new Collection(id: 'playlist'));
        $database->createCollection(new Collection(id: 'song'));

        $database->createAttribute('playlist', Attribute::string(key: 'name', required: true));
        $database->createAttribute('song', Attribute::string(key: 'name', required: true));
        $database->createAttribute('song', Attribute::integer(key: 'length', required: true));

        $database->createRelationship(Relationship::manyToMany(collection: 'playlist', relatedCollection: 'song', key: 'songs'));

        // Check metadata for collection
        $collection = $database->getCollection('playlist');

        foreach ($collection->attributes as $attribute) {
            if ($attribute->key === 'songs') {
                $options = $attribute->options ?? [];
                $this->assertEquals(ColumnType::Relationship, $attribute->type);
                $this->assertEquals('songs', $attribute->getId());
                $this->assertEquals('songs', $attribute->key);
                $this->assertEquals('song', $options['relatedCollection'] ?? null);
                $this->assertEquals(RelationType::ManyToMany->value, $options['relationType'] ?? null);
                $this->assertEquals(false, $options['twoWay'] ?? null);
                $this->assertEquals('playlist', $options['twoWayKey'] ?? null);
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
                'song2',
            ],
        ]));

        // Update a document with non existing related document. It should not get added to the list.
        $database->updateDocument('playlist', 'playlist1', $playlist1->setAttribute('songs', ['song1', 'no-song']));

        $playlist1Document = $database->getDocument('playlist', 'playlist1');
        // Assert document does not contain non existing relation document.
        $this->assertEquals(1, \count($playlist1Document->getDocuments('songs')));

        $documents = $database->find('playlist', [
            Query::select(['name']),
            Query::limit(1),
        ]);

        $this->assertArrayNotHasKey('songs', $documents[0]);

        // Get document with relationship
        $playlist = $database->getDocument('playlist', 'playlist1');
        $songs = $playlist->getDocuments('songs');
        $this->assertNotEmpty($songs);
        $this->assertEquals('song1', $songs[0]->getId());
        $this->assertArrayNotHasKey('playlist', $songs[0]);

        $playlist = $database->getDocument('playlist', 'playlist2');
        $songs = $playlist->getDocuments('songs');
        $this->assertNotEmpty($songs);
        $this->assertEquals('song2', $songs[0]->getId());
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
            Query::select(['*', 'songs.name']),
        ]);

        if ($playlist->isEmpty()) {
            throw new Exception('Playlist not found');
        }

        $songs = $playlist->getDocuments('songs');
        $this->assertNotEmpty($songs);
        $this->assertEquals('Song 1', $songs[0]->getAttribute('name'));
        $this->assertArrayNotHasKey('length', $songs[0]);

        $playlist = $database->getDocument('playlist', 'playlist1', [
            Query::select(['*', 'songs.name']),
        ]);

        $songs = $playlist->getDocuments('songs');
        $this->assertNotEmpty($songs);
        $this->assertEquals('Song 1', $songs[0]->getAttribute('name'));
        $this->assertArrayNotHasKey('length', $songs[0]);

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
        $songs = $playlist1->getDocuments('songs');
        $this->assertNotEmpty($songs);
        $songs[0]->setAttribute('name', 'Song 1 Updated');

        $playlist1 = $database->updateDocument(
            'playlist',
            $playlist1->getId(),
            $playlist1->setAttribute('songs', $songs)
        );

        $songs = $playlist1->getDocuments('songs');
        $this->assertNotEmpty($songs);
        $this->assertEquals('Song 1 Updated', $songs[0]->getAttribute('name'));
        $playlist1 = $database->getDocument('playlist', 'playlist1');
        $songs = $playlist1->getDocuments('songs');
        $this->assertNotEmpty($songs);
        $this->assertEquals('Song 1 Updated', $songs[0]->getAttribute('name'));

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
                'song5',
            ],
        ]));

        $songs = $playlist5->getDocuments('songs');
        $this->assertNotEmpty($songs);
        $this->assertEquals('Song 5', $songs[0]->getAttribute('name'));
        $playlist5 = $database->getDocument('playlist', 'playlist5');
        $songs = $playlist5->getDocuments('songs');
        $this->assertNotEmpty($songs);
        $this->assertEquals('Song 5', $songs[0]->getAttribute('name'));

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
        $songs = $playlist->getDocuments('newSongs');
        $this->assertNotEmpty($songs);
        $this->assertEquals('song2', $songs[0]->getId());

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
            onDelete: ForeignKeyAction::SetNull
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
        $this->assertEquals(0, \count($playlist1->getDocuments('newSongs')));

        // Change on delete to cascade
        $database->updateRelationship(
            collection: 'playlist',
            id: 'newSongs',
            onDelete: ForeignKeyAction::Cascade
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
        $database = $this->getDatabase();

        if (! ($database->getAdapter()->hasFeature(Feature\Relationships::class))) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection(new Collection(id: 'students'));
        $database->createCollection(new Collection(id: 'classes'));

        $database->createAttribute('students', Attribute::string(key: 'name', required: true));
        $database->createAttribute('classes', Attribute::string(key: 'name', required: true));
        $database->createAttribute('classes', Attribute::integer(key: 'number', required: true));

        $database->createRelationship(Relationship::manyToMany(collection: 'students', relatedCollection: 'classes', twoWay: true));

        // Check metadata for collection
        $collection = $database->getCollection('students');
        foreach ($collection->attributes as $attribute) {
            if ($attribute->key === 'students') {
                $options = $attribute->options ?? [];
                $this->assertEquals(ColumnType::Relationship, $attribute->type);
                $this->assertEquals('students', $attribute->getId());
                $this->assertEquals('students', $attribute->key);
                $this->assertEquals('students', $options['relatedCollection'] ?? null);
                $this->assertEquals(RelationType::ManyToMany->value, $options['relationType'] ?? null);
                $this->assertEquals(true, $options['twoWay'] ?? null);
                $this->assertEquals('classes', $options['twoWayKey'] ?? null);
            }
        }

        // Check metadata for related collection
        $collection = $database->getCollection('classes');
        foreach ($collection->attributes as $attribute) {
            if ($attribute->key === 'classes') {
                $options = $attribute->options ?? [];
                $this->assertEquals(ColumnType::Relationship, $attribute->type);
                $this->assertEquals('classes', $attribute->getId());
                $this->assertEquals('classes', $attribute->key);
                $this->assertEquals('classes', $options['relatedCollection'] ?? null);
                $this->assertEquals(RelationType::ManyToMany->value, $options['relationType'] ?? null);
                $this->assertEquals(true, $options['twoWay'] ?? null);
                $this->assertEquals('students', $options['twoWayKey'] ?? null);
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
        $this->assertEquals(1, \count($student1Document->getDocuments('classes')));

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
                'class2',
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
                ],
            ],
        ]));
        $database->createDocument('students', new Document([
            '$id' => 'student4',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Student 4',
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
                'student4',
            ],
        ]));

        // Get document with relationship
        $student = $database->getDocument('students', 'student1');
        $classes = $student->getDocuments('classes');
        $this->assertNotEmpty($classes);
        $this->assertEquals('class1', $classes[0]->getId());
        $this->assertArrayNotHasKey('students', $classes[0]);

        $student = $database->getDocument('students', 'student2');
        $classes = $student->getDocuments('classes');
        $this->assertNotEmpty($classes);
        $this->assertEquals('class2', $classes[0]->getId());
        $this->assertArrayNotHasKey('students', $classes[0]);

        $student = $database->getDocument('students', 'student3');
        $classes = $student->getDocuments('classes');
        $this->assertNotEmpty($classes);
        $this->assertEquals('class3', $classes[0]->getId());
        $this->assertArrayNotHasKey('students', $classes[0]);

        $student = $database->getDocument('students', 'student4');
        $classes = $student->getDocuments('classes');
        $this->assertNotEmpty($classes);
        $this->assertEquals('class4', $classes[0]->getId());
        $this->assertArrayNotHasKey('students', $classes[0]);

        // Get related document
        $class = $database->getDocument('classes', 'class1');
        $students = $class->getDocuments('students');
        $this->assertNotEmpty($students);
        $this->assertEquals('student1', $students[0]->getId());
        $this->assertArrayNotHasKey('classes', $students[0]);

        $class = $database->getDocument('classes', 'class2');
        $students = $class->getDocuments('students');
        $this->assertNotEmpty($students);
        $this->assertEquals('student2', $students[0]->getId());
        $this->assertArrayNotHasKey('classes', $students[0]);

        $class = $database->getDocument('classes', 'class3');
        $students = $class->getDocuments('students');
        $this->assertNotEmpty($students);
        $this->assertEquals('student3', $students[0]->getId());
        $this->assertArrayNotHasKey('classes', $students[0]);

        $class = $database->getDocument('classes', 'class4');
        $students = $class->getDocuments('students');
        $this->assertNotEmpty($students);
        $this->assertEquals('student4', $students[0]->getId());
        $this->assertArrayNotHasKey('classes', $students[0]);

        // Select related document attributes
        $student = $database->findOne('students', [
            Query::select(['*', 'classes.name']),
        ]);

        if ($student->isEmpty()) {
            throw new Exception('Student not found');
        }

        $classes = $student->getDocuments('classes');
        $this->assertNotEmpty($classes);
        $this->assertEquals('Class 1', $classes[0]->getAttribute('name'));
        $this->assertArrayNotHasKey('number', $classes[0]);

        $student = $database->getDocument('students', 'student1', [
            Query::select(['*', 'classes.name']),
        ]);

        $classes = $student->getDocuments('classes');
        $this->assertNotEmpty($classes);
        $this->assertEquals('Class 1', $classes[0]->getAttribute('name'));
        $this->assertArrayNotHasKey('number', $classes[0]);

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
        $classes = $student1->getDocuments('classes');
        $this->assertNotEmpty($classes);
        $classes[0]->setAttribute('name', 'Class 1 Updated');

        $student1 = $database->updateDocument(
            'students',
            $student1->getId(),
            $student1->setAttribute('classes', $classes)
        );

        $classes = $student1->getDocuments('classes');
        $this->assertNotEmpty($classes);
        $this->assertEquals('Class 1 Updated', $classes[0]->getAttribute('name'));
        $student1 = $database->getDocument('students', 'student1');
        $classes = $student1->getDocuments('classes');
        $this->assertNotEmpty($classes);
        $this->assertEquals('Class 1 Updated', $classes[0]->getAttribute('name'));

        // Update inverse nested document attribute
        $students = $class2->getDocuments('students');
        $this->assertNotEmpty($students);
        $students[0]->setAttribute('name', 'Student 2 Updated');

        $class2 = $database->updateDocument(
            'classes',
            $class2->getId(),
            $class2->setAttribute('students', $students)
        );

        $students = $class2->getDocuments('students');
        $this->assertNotEmpty($students);
        $this->assertEquals('Student 2 Updated', $students[0]->getAttribute('name'));
        $class2 = $database->getDocument('classes', 'class2');
        $students = $class2->getDocuments('students');
        $this->assertNotEmpty($students);
        $this->assertEquals('Student 2 Updated', $students[0]->getAttribute('name'));

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

        $classes = $student5->getDocuments('classes');
        $this->assertNotEmpty($classes);
        $this->assertEquals('Class 5', $classes[0]->getAttribute('name'));
        $student5 = $database->getDocument('students', 'student5');
        $classes = $student5->getDocuments('classes');
        $this->assertNotEmpty($classes);
        $this->assertEquals('Class 5', $classes[0]->getAttribute('name'));

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

        $students = $class6->getDocuments('students');
        $this->assertNotEmpty($students);
        $this->assertEquals('Student 6', $students[0]->getAttribute('name'));
        $class6 = $database->getDocument('classes', 'class6');
        $students = $class6->getDocuments('students');
        $this->assertNotEmpty($students);
        $this->assertEquals('Student 6', $students[0]->getAttribute('name'));

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
        $student = $database->getDocument('students', 'student1');
        $classes = $student->getDocuments('newClasses');
        $this->assertNotEmpty($classes);
        $this->assertEquals('class2', $classes[0]->getId());

        // Get inverse document with new relationship key
        $class = $database->getDocument('classes', 'class1');
        $students = $class->getDocuments('newStudents');
        $this->assertNotEmpty($students);
        $this->assertEquals('student1', $students[0]->getId());

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
            onDelete: ForeignKeyAction::SetNull
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
        $this->assertEquals(0, \count($student1->getDocuments('newClasses')));

        // Change on delete to cascade
        $database->updateRelationship(
            collection: 'students',
            id: 'newClasses',
            onDelete: ForeignKeyAction::Cascade
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
        $database = $this->getDatabase();

        if (! ($database->getAdapter()->hasFeature(Feature\Relationships::class))) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection(new Collection(id: 'stones'));
        $database->createCollection(new Collection(id: 'hearths'));
        $database->createCollection(new Collection(id: 'plots'));

        $database->createAttribute('stones', Attribute::string(key: 'name', required: true));
        $database->createAttribute('hearths', Attribute::string(key: 'name', required: true));
        $database->createAttribute('plots', Attribute::string(key: 'name', required: true));

        $database->createRelationship(Relationship::manyToMany(collection: 'stones', relatedCollection: 'hearths', twoWay: true));
        $database->createRelationship(Relationship::oneToOne(
            collection: 'hearths',
            relatedCollection: 'plots',
            twoWay: true,
            key: 'plot',
            twoWayKey: 'hearth'
        ));

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
        $hearths = $stone1->getDocuments('hearths');
        $this->assertCount(2, $hearths);
        $this->assertEquals('hearth1', $hearths[0]->getId());
        $this->assertEquals('hearth2', $hearths[1]->getId());
        $this->assertArrayNotHasKey('stone', $hearths[0]);
        $this->assertEquals('plot1', $hearths[0]->getDocument('plot')->getId());
        $this->assertEquals('plot2', $hearths[1]->getDocument('plot')->getId());
        $this->assertArrayNotHasKey('hearth', $hearths[0]->getDocument('plot'));

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
        $hearth = $plot3->getDocument('hearth');
        $this->assertEquals('hearth3', $hearth->getId());
        $this->assertArrayNotHasKey('plot', $hearth);
        $stones = $hearth->getDocuments('stones');
        $this->assertNotEmpty($stones);
        $this->assertEquals('stone2', $stones[0]->getId());
        $this->assertArrayNotHasKey('hearths', $stones[0]);
    }

    public function testNestedManyToMany_OneToManyRelationship(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! ($database->getAdapter()->hasFeature(Feature\Relationships::class))) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection(new Collection(id: 'groups'));
        $database->createCollection(new Collection(id: 'tounaments'));
        $database->createCollection(new Collection(id: 'prizes'));

        $database->createAttribute('groups', Attribute::string(key: 'name', required: true));
        $database->createAttribute('tounaments', Attribute::string(key: 'name', required: true));
        $database->createAttribute('prizes', Attribute::string(key: 'name', required: true));

        $database->createRelationship(Relationship::manyToMany(collection: 'groups', relatedCollection: 'tounaments', twoWay: true));
        $database->createRelationship(Relationship::oneToMany(
            collection: 'tounaments',
            relatedCollection: 'prizes',
            twoWay: true,
            key: 'prizes',
            twoWayKey: 'tounament'
        ));

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
        $tounaments = $group1->getDocuments('tounaments');
        $this->assertCount(2, $tounaments);
        $this->assertEquals('tounament1', $tounaments[0]->getId());
        $this->assertEquals('tounament2', $tounaments[1]->getId());
        $this->assertArrayNotHasKey('group', $tounaments[0]);
        $prizes = $tounaments[0]->getDocuments('prizes');
        $this->assertCount(2, $prizes);
        $this->assertEquals('prize1', $prizes[0]->getId());
        $this->assertEquals('prize2', $prizes[1]->getId());
        $this->assertArrayNotHasKey('tounament', $prizes[0]);
    }

    public function testNestedManyToMany_ManyToOneRelationship(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! ($database->getAdapter()->hasFeature(Feature\Relationships::class))) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection(new Collection(id: 'platforms'));
        $database->createCollection(new Collection(id: 'games'));
        $database->createCollection(new Collection(id: 'publishers'));

        $database->createAttribute('platforms', Attribute::string(key: 'name', required: true));
        $database->createAttribute('games', Attribute::string(key: 'name', required: true));
        $database->createAttribute('publishers', Attribute::string(key: 'name', required: true));

        $database->createRelationship(Relationship::manyToMany(collection: 'platforms', relatedCollection: 'games', twoWay: true));
        $database->createRelationship(Relationship::manyToOne(
            collection: 'games',
            relatedCollection: 'publishers',
            twoWay: true,
            key: 'publisher',
            twoWayKey: 'games'
        ));

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
            ],
        ]));

        $platform1 = $database->getDocument('platforms', 'platform1');
        $games = $platform1->getDocuments('games');
        $this->assertCount(2, $games);
        $this->assertEquals('game1', $games[0]->getId());
        $this->assertEquals('game2', $games[1]->getId());
        $this->assertArrayNotHasKey('platforms', $games[0]);
        $this->assertEquals('publisher1', $games[0]->getDocument('publisher')->getId());
        $this->assertEquals('publisher2', $games[1]->getDocument('publisher')->getId());
        $this->assertArrayNotHasKey('games', $games[0]->getDocument('publisher'));

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
                        ],
                    ],
                ],
            ],
        ]));

        $publisher3 = $database->getDocument('publishers', 'publisher3');
        $games = $publisher3->getDocuments('games');
        $this->assertCount(1, $games);
        $this->assertEquals('game3', $games[0]->getId());
        $this->assertArrayNotHasKey('publisher', $games[0]);
        $platforms = $games[0]->getDocuments('platforms');
        $this->assertNotEmpty($platforms);
        $this->assertEquals('platform2', $platforms[0]->getId());
        $this->assertArrayNotHasKey('games', $platforms[0]);
    }

    public function testNestedManyToMany_ManyToManyRelationship(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! ($database->getAdapter()->hasFeature(Feature\Relationships::class))) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection(new Collection(id: 'sauces'));
        $database->createCollection(new Collection(id: 'pizzas'));
        $database->createCollection(new Collection(id: 'toppings'));

        $database->createAttribute('sauces', Attribute::string(key: 'name', required: true));
        $database->createAttribute('pizzas', Attribute::string(key: 'name', required: true));
        $database->createAttribute('toppings', Attribute::string(key: 'name', required: true));

        $database->createRelationship(Relationship::manyToMany(collection: 'sauces', relatedCollection: 'pizzas', twoWay: true));
        $database->createRelationship(Relationship::manyToMany(
            collection: 'pizzas',
            relatedCollection: 'toppings',
            twoWay: true,
            key: 'toppings',
            twoWayKey: 'pizzas'
        ));

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
            ],
        ]));

        $sauce1 = $database->getDocument('sauces', 'sauce1');
        $pizzas = $sauce1->getDocuments('pizzas');
        $this->assertCount(2, $pizzas);
        $this->assertEquals('pizza1', $pizzas[0]->getId());
        $this->assertEquals('pizza2', $pizzas[1]->getId());
        $this->assertArrayNotHasKey('sauces', $pizzas[0]);
        $pizza1Toppings = $pizzas[0]->getDocuments('toppings');
        $this->assertCount(2, $pizza1Toppings);
        $this->assertEquals('topping1', $pizza1Toppings[0]->getId());
        $this->assertEquals('topping2', $pizza1Toppings[1]->getId());
        $this->assertArrayNotHasKey('pizzas', $pizza1Toppings[0]);
        $pizza2Toppings = $pizzas[1]->getDocuments('toppings');
        $this->assertCount(2, $pizza2Toppings);
        $this->assertEquals('topping3', $pizza2Toppings[0]->getId());
        $this->assertEquals('topping4', $pizza2Toppings[1]->getId());
        $this->assertArrayNotHasKey('pizzas', $pizza2Toppings[0]);
    }

    public function testManyToManyRelationshipKeyWithSymbols(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! ($database->getAdapter()->hasFeature(Feature\Relationships::class))) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection(new Collection(id: '$symbols_coll.ection7'));
        $database->createCollection(new Collection(id: '$symbols_coll.ection8'));

        $database->createRelationship(Relationship::manyToMany(collection: '$symbols_coll.ection7', relatedCollection: '$symbols_coll.ection8', twoWay: true));

        $doc1 = $database->createDocument('$symbols_coll.ection8', new Document([
            '$id' => ID::unique(),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
        ]));
        $doc2 = $database->createDocument('$symbols_coll.ection7', new Document([
            '$id' => ID::unique(),
            'symbols_collection8' => [$doc1->getId()],
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
        ]));

        $doc1 = $database->getDocument('$symbols_coll.ection8', $doc1->getId());
        $doc2 = $database->getDocument('$symbols_coll.ection7', $doc2->getId());

        $relatedFromDoc1 = $doc1->getDocuments('symbols_collection7');
        $this->assertNotEmpty($relatedFromDoc1);
        $this->assertEquals($doc2->getId(), $relatedFromDoc1[0]->getId());
        $relatedFromDoc2 = $doc2->getDocuments('symbols_collection8');
        $this->assertNotEmpty($relatedFromDoc2);
        $this->assertEquals($doc1->getId(), $relatedFromDoc2[0]->getId());
    }

    public function testRecreateManyToManyOneWayRelationshipFromChild(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! ($database->getAdapter()->hasFeature(Feature\Relationships::class))) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $one = 'one_' . uniqid();
        $two = 'two_' . uniqid();

        $database->createCollection(new Collection(id: $one, attributes: [
            Attribute::string(key: 'name', size: 100, format: ''),
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ]));
        $database->createCollection(new Collection(id: $two, attributes: [
            Attribute::string(key: 'name', size: 100, format: ''),
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ]));

        $database->createRelationship(Relationship::manyToMany(collection: $one, relatedCollection: $two));

        $database->deleteRelationship($two, $one);

        $result = $database->createRelationship(Relationship::manyToMany(collection: $one, relatedCollection: $two));

        $this->assertTrue($result);

        $database->deleteCollection($one);
        $database->deleteCollection($two);
    }

    public function testRecreateManyToManyTwoWayRelationshipFromParent(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! ($database->getAdapter()->hasFeature(Feature\Relationships::class))) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $one = 'one_' . uniqid();
        $two = 'two_' . uniqid();

        $database->createCollection(new Collection(id: $one, attributes: [
            Attribute::string(key: 'name', size: 100, format: ''),
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ]));
        $database->createCollection(new Collection(id: $two, attributes: [
            Attribute::string(key: 'name', size: 100, format: ''),
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ]));

        $database->createRelationship(Relationship::manyToMany(collection: $one, relatedCollection: $two, twoWay: true));

        $database->deleteRelationship($one, $two);

        $result = $database->createRelationship(Relationship::manyToMany(collection: $one, relatedCollection: $two, twoWay: true));

        $this->assertTrue($result);

        $database->deleteCollection($one);
        $database->deleteCollection($two);
    }

    public function testRecreateManyToManyTwoWayRelationshipFromChild(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! ($database->getAdapter()->hasFeature(Feature\Relationships::class))) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $one = 'one_' . uniqid();
        $two = 'two_' . uniqid();

        $database->createCollection(new Collection(id: $one, attributes: [
            Attribute::string(key: 'name', size: 100, format: ''),
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ]));
        $database->createCollection(new Collection(id: $two, attributes: [
            Attribute::string(key: 'name', size: 100, format: ''),
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ]));

        $database->createRelationship(Relationship::manyToMany(collection: $one, relatedCollection: $two, twoWay: true));

        $database->deleteRelationship($two, $one);

        $result = $database->createRelationship(Relationship::manyToMany(collection: $one, relatedCollection: $two, twoWay: true));

        $this->assertTrue($result);

        $database->deleteCollection($one);
        $database->deleteCollection($two);
    }

    public function testRecreateManyToManyOneWayRelationshipFromParent(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! ($database->getAdapter()->hasFeature(Feature\Relationships::class))) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $one = 'one_' . uniqid();
        $two = 'two_' . uniqid();

        $database->createCollection(new Collection(id: $one, attributes: [
            Attribute::string(key: 'name', size: 100, format: ''),
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ]));
        $database->createCollection(new Collection(id: $two, attributes: [
            Attribute::string(key: 'name', size: 100, format: ''),
        ], permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ]));

        $database->createRelationship(Relationship::manyToMany(collection: $one, relatedCollection: $two));

        $database->deleteRelationship($one, $two);

        $result = $database->createRelationship(Relationship::manyToMany(collection: $one, relatedCollection: $two));

        $this->assertTrue($result);

        $database->deleteCollection($one);
        $database->deleteCollection($two);
    }

    public function testSelectManyToMany(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! ($database->getAdapter()->hasFeature(Feature\Relationships::class))) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection(new Collection(id: 'select_m2m_collection1'));
        $database->createCollection(new Collection(id: 'select_m2m_collection2'));

        $database->createAttribute('select_m2m_collection1', Attribute::string(key: 'name', required: true));
        $database->createAttribute('select_m2m_collection1', Attribute::string(key: 'type', required: true));
        $database->createAttribute('select_m2m_collection2', Attribute::string(key: 'name', required: true));
        $database->createAttribute('select_m2m_collection2', Attribute::string(key: 'type', required: true));

        // Many-to-Many Relationship
        $database->createRelationship(Relationship::manyToMany(collection: 'select_m2m_collection1', relatedCollection: 'select_m2m_collection2', twoWay: true));

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

        $relatedDocs = $docs[0]->getDocuments('select_m2m_collection2');

        $this->assertCount(2, $relatedDocs);
        $this->assertEquals('Related Document 1', $relatedDocs[0]->getAttribute('name'));
        $this->assertEquals('Related Document 2', $relatedDocs[1]->getAttribute('name'));
        $this->assertArrayNotHasKey('type', $relatedDocs[0]);
        $this->assertArrayNotHasKey('type', $relatedDocs[1]);
    }

    public function testSelectAcrossMultipleCollections(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! ($database->getAdapter()->hasFeature(Feature\Relationships::class))) {
            $this->expectNotToPerformAssertions();

            return;
        }

        // Create collections
        $database->createCollection(new Collection(id: 'artists', permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ], documentSecurity: false));
        $database->createCollection(new Collection(id: 'albums', permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ], documentSecurity: false));
        $database->createCollection(new Collection(id: 'tracks', permissions: [
            Permission::read(Role::any()),
            Permission::create(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ], documentSecurity: false));

        // Add attributes
        $database->createAttribute('artists', Attribute::string(key: 'name', required: true));
        $database->createAttribute('albums', Attribute::string(key: 'name', required: true));
        $database->createAttribute('tracks', Attribute::string(key: 'title', required: true));
        $database->createAttribute('tracks', Attribute::integer(key: 'duration', required: true));

        // Create relationships
        $database->createRelationship(Relationship::manyToMany(collection: 'artists', relatedCollection: 'albums', twoWay: true));

        $database->createRelationship(Relationship::manyToMany(collection: 'albums', relatedCollection: 'tracks', twoWay: true));

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
                        ],
                    ],
                ],
                [
                    '$id' => 'album2',
                    'name' => 'Second Album',
                    'tracks' => [
                        [
                            '$id' => 'track3',
                            'title' => 'Ballad 3',
                            'duration' => 240,
                        ],
                    ],
                ],
            ],
        ]));

        // Query with nested select
        $artists = $database->find('artists', [
            Query::select(['name', 'albums.name', 'albums.tracks.title']),
        ]);

        $this->assertCount(1, $artists);
        $artist = $artists[0];
        $this->assertEquals('The Great Artist', $artist->getAttribute('name'));
        $this->assertArrayHasKey('albums', $artist->getArrayCopy());

        $albums = $artist->getDocuments('albums');
        $this->assertCount(2, $albums);

        $album1 = $albums[0];
        $this->assertEquals('First Album', $album1->getAttribute('name'));
        $this->assertArrayHasKey('tracks', $album1);
        $this->assertArrayNotHasKey('artists', $album1);

        $album2 = $albums[1];
        $this->assertEquals('Second Album', $album2->getAttribute('name'));
        $this->assertArrayHasKey('tracks', $album2);

        $album1Tracks = $album1->getDocuments('tracks');
        $this->assertCount(2, $album1Tracks);
        $this->assertEquals('Hit Song 1', $album1Tracks[0]->getAttribute('title'));
        $this->assertArrayNotHasKey('duration', $album1Tracks[0]);
        $this->assertEquals('Hit Song 2', $album1Tracks[1]->getAttribute('title'));
        $this->assertArrayNotHasKey('duration', $album1Tracks[1]);

        $album2Tracks = $album2->getDocuments('tracks');
        $this->assertCount(1, $album2Tracks);
        $this->assertEquals('Ballad 3', $album2Tracks[0]->getAttribute('title'));
        $this->assertArrayNotHasKey('duration', $album2Tracks[0]);
    }

    public function testDeleteBulkDocumentsManyToManyRelationship(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! ($database->getAdapter()->hasFeature(Feature\Relationships::class)) || ! $database->getAdapter()->supports(Capability::BatchOperations)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $this->getDatabase()->createCollection(new Collection(id: 'bulk_delete_person_m2m'));
        $this->getDatabase()->createCollection(new Collection(id: 'bulk_delete_library_m2m'));

        $this->getDatabase()->createAttribute('bulk_delete_person_m2m', Attribute::string(key: 'name', required: true));
        $this->getDatabase()->createAttribute('bulk_delete_library_m2m', Attribute::string(key: 'name', required: true));
        $this->getDatabase()->createAttribute('bulk_delete_library_m2m', Attribute::string(key: 'area', required: true));

        // Many-to-Many Relationship
        $this->getDatabase()->createRelationship(Relationship::manyToMany(collection: 'bulk_delete_person_m2m', relatedCollection: 'bulk_delete_library_m2m'));

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
        $libraries = $person1->getDocuments('bulk_delete_library_m2m');
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
        $database = $this->getDatabase();

        if (
            ! ($database->getAdapter()->hasFeature(Feature\Relationships::class)) ||
            ! $database->getAdapter()->supports(Capability::BatchOperations)
        ) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $parentCollection = 'parent_combined_m2m';
        $childCollection = 'child_combined_m2m';

        $database->createCollection(new Collection(id: $parentCollection));
        $database->createCollection(new Collection(id: $childCollection));

        $database->createAttribute($parentCollection, Attribute::string(key: 'name', required: true));
        $database->createAttribute($childCollection, Attribute::string(key: 'name', required: true));
        $database->createAttribute($childCollection, Attribute::integer(key: 'parentNumber'));

        $database->createRelationship(Relationship::manyToMany(collection: $parentCollection, relatedCollection: $childCollection, key: 'parentNumber'));

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
        $database = $this->getDatabase();

        if (! ($database->getAdapter()->hasFeature(Feature\Relationships::class)) || ! $database->getAdapter()->supports(Capability::BatchOperations)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $parentCollection = 'parent_relationship_many_to_many';
        $childCollection = 'child_relationship_many_to_many';

        $database->createCollection(new Collection(id: $parentCollection));
        $database->createCollection(new Collection(id: $childCollection));
        $database->createAttribute($parentCollection, Attribute::string(key: 'name', required: true));
        $database->createAttribute($childCollection, Attribute::string(key: 'name', required: true));

        $database->createRelationship(Relationship::manyToMany(collection: $parentCollection, relatedCollection: $childCollection));

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

    public function testPartialUpdateManyToManyBothSides(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (! ($database->getAdapter()->hasFeature(Feature\Relationships::class))) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection(new Collection(id: 'partial_students'));
        $database->createCollection(new Collection(id: 'partial_courses'));

        $database->createAttribute('partial_students', Attribute::string(key: 'name', required: true));
        $database->createAttribute('partial_students', Attribute::string(key: 'grade', size: 10));
        $database->createAttribute('partial_courses', Attribute::string(key: 'title', required: true));
        $database->createAttribute('partial_courses', Attribute::integer(key: 'credits'));

        $database->createRelationship(Relationship::manyToMany(
            collection: 'partial_students',
            relatedCollection: 'partial_courses',
            twoWay: true,
            key: 'partial_courses',
            twoWayKey: 'partial_students'
        ));

        // Create student with courses
        $database->createDocument('partial_students', new Document([
            '$id' => 'student1',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'name' => 'David',
            'grade' => 'A',
            'partial_courses' => [
                ['$id' => 'course1', '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())], 'title' => 'Math', 'credits' => 3],
                ['$id' => 'course2', '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())], 'title' => 'Science', 'credits' => 4],
            ],
        ]));

        // Partial update from student side - update grade only, preserve courses
        $database->updateDocument('partial_students', 'student1', new Document([
            '$id' => 'student1',
            '$collection' => 'partial_students',
            'grade' => 'A+',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
        ]));

        $student = $database->getDocument('partial_students', 'student1');
        $this->assertEquals('David', $student->getAttribute('name'), 'Name should be preserved');
        $this->assertEquals('A+', $student->getAttribute('grade'), 'Grade should be updated');
        $this->assertCount(2, $student->getDocuments('partial_courses'), 'Courses should be preserved');

        // Partial update from course side - update credits only, preserve students
        $database->updateDocument('partial_courses', 'course1', new Document([
            '$id' => 'course1',
            '$collection' => 'partial_courses',
            'credits' => 5,
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
        ]));

        $course = $database->getDocument('partial_courses', 'course1');
        $this->assertEquals('Math', $course->getAttribute('title'), 'Title should be preserved');
        $this->assertEquals(5, $course->getAttribute('credits'), 'Credits should be updated');
        $this->assertCount(1, $course->getDocuments('partial_students'), 'Students should be preserved');

        $database->deleteCollection('partial_students');
        $database->deleteCollection('partial_courses');
    }

    public function testPartialUpdateManyToManyWithStringIdsAndDocuments(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (! ($database->getAdapter()->hasFeature(Feature\Relationships::class))) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->createCollection(new Collection(id: 'tags'));
        $database->createCollection(new Collection(id: 'articles'));

        $database->createAttribute('tags', Attribute::string(key: 'name', required: true));
        $database->createAttribute('tags', Attribute::string(key: 'color', size: 50));
        $database->createAttribute('articles', Attribute::string(key: 'title', required: true));
        $database->createAttribute('articles', Attribute::boolean(key: 'published'));

        $database->createRelationship(Relationship::manyToMany(
            collection: 'articles',
            relatedCollection: 'tags',
            twoWay: true,
            key: 'tags',
            twoWayKey: 'articles'
        ));

        // Create article with tags
        $database->createDocument('articles', new Document([
            '$id' => 'article1',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'title' => 'Great Article',
            'published' => false,
            'tags' => [
                ['$id' => 'tag1', '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())], 'name' => 'Tech', 'color' => 'blue'],
            ],
        ]));

        $database->createDocument('tags', new Document([
            '$id' => 'tag2',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'name' => 'News',
            'color' => 'red',
        ]));

        // Update using STRING IDs
        $database->updateDocument('articles', 'article1', new Document([
            '$id' => 'article1',
            '$collection' => 'articles',
            'tags' => ['tag1', 'tag2'], // String IDs
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
        ]));

        $article = $database->getDocument('articles', 'article1');
        $this->assertEquals('Great Article', $article->getAttribute('title'));
        $this->assertFalse($article->getAttribute('published'));
        $this->assertCount(2, $article->getDocuments('tags'));

        // Update from tag side using DOCUMENT objects
        $database->createDocument('articles', new Document([
            '$id' => 'article2',
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
            'title' => 'Another Article',
            'published' => true,
        ]));

        $database->updateDocument('tags', 'tag1', new Document([
            '$id' => 'tag1',
            '$collection' => 'tags',
            'articles' => [ // Document objects
                new Document(['$id' => 'article1']),
                new Document(['$id' => 'article2']),
            ],
            '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())],
        ]));

        $tag = $database->getDocument('tags', 'tag1');
        $this->assertEquals('Tech', $tag->getAttribute('name'));
        $this->assertEquals('blue', $tag->getAttribute('color'));
        $this->assertCount(2, $tag->getDocuments('articles'));

        $database->deleteCollection('tags');
        $database->deleteCollection('articles');
    }

    /**
     * Regression: processNestedRelationshipPath used skipRelationships()
     * for many-to-many reverse lookups, which prevented junction-table data
     * (twoWayKey) from being populated, yielding empty matchingIds.
     */
    public function testNestedManyToManyRelationshipQueries(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! ($database->getAdapter()->hasFeature(Feature\Relationships::class))) {
            $this->expectNotToPerformAssertions();

            return;
        }

        // Clean up if collections already exist from other tests
        foreach (['brands', 'products', 'tags'] as $col) {
            try {
                $database->deleteCollection($col);
            } catch (\Throwable) {
            }
        }

        // 3-level many-to-many chain: brands <-> products <-> tags
        $database->createCollection(new Collection(id: 'brands'));
        $database->createCollection(new Collection(id: 'products'));
        $database->createCollection(new Collection(id: 'tags'));

        $database->createAttribute('brands', Attribute::string(key: 'name', required: true));
        $database->createAttribute('products', Attribute::string(key: 'title', required: true));
        $database->createAttribute('tags', Attribute::string(key: 'label', required: true));

        $database->createRelationship(Relationship::manyToMany(
            collection: 'brands',
            relatedCollection: 'products',
            twoWay: true,
            key: 'products',
            twoWayKey: 'brands'
        ));

        $database->createRelationship(Relationship::manyToMany(
            collection: 'products',
            relatedCollection: 'tags',
            twoWay: true,
            key: 'tags',
            twoWayKey: 'products'
        ));

        // Seed data
        $database->createDocument('tags', new Document([
            '$id' => 'tag_eco',
            '$permissions' => [Permission::read(Role::any())],
            'label' => 'Eco-Friendly',
        ]));
        $database->createDocument('tags', new Document([
            '$id' => 'tag_premium',
            '$permissions' => [Permission::read(Role::any())],
            'label' => 'Premium',
        ]));
        $database->createDocument('tags', new Document([
            '$id' => 'tag_sale',
            '$permissions' => [Permission::read(Role::any())],
            'label' => 'Sale',
        ]));

        $database->createDocument('products', new Document([
            '$id' => 'prod_a',
            '$permissions' => [Permission::read(Role::any())],
            'title' => 'Product A',
            'tags' => ['tag_eco', 'tag_premium'],
        ]));
        $database->createDocument('products', new Document([
            '$id' => 'prod_b',
            '$permissions' => [Permission::read(Role::any())],
            'title' => 'Product B',
            'tags' => ['tag_sale'],
        ]));
        $database->createDocument('products', new Document([
            '$id' => 'prod_c',
            '$permissions' => [Permission::read(Role::any())],
            'title' => 'Product C',
            'tags' => ['tag_eco'],
        ]));

        $database->createDocument('brands', new Document([
            '$id' => 'brand_x',
            '$permissions' => [Permission::read(Role::any())],
            'name' => 'BrandX',
            'products' => ['prod_a', 'prod_b'],
        ]));
        $database->createDocument('brands', new Document([
            '$id' => 'brand_y',
            '$permissions' => [Permission::read(Role::any())],
            'name' => 'BrandY',
            'products' => ['prod_c'],
        ]));

        // 1-level deep: query brands by product title (many-to-many)
        $brands = $database->find('brands', [
            Query::equal('products.title', ['Product A']),
        ]);
        $this->assertCount(1, $brands);
        $this->assertEquals('brand_x', $brands[0]->getId());

        // 2-level deep: query brands by product→tag label (many-to-many→many-to-many)
        // "Eco-Friendly" tag is on prod_a (BrandX) and prod_c (BrandY)
        $brands = $database->find('brands', [
            Query::equal('products.tags.label', ['Eco-Friendly']),
        ]);
        $this->assertCount(2, $brands);
        $brandIds = \array_map(fn ($d) => $d->getId(), $brands);
        $this->assertContains('brand_x', $brandIds);
        $this->assertContains('brand_y', $brandIds);

        // "Sale" tag is only on prod_b (BrandX)
        $brands = $database->find('brands', [
            Query::equal('products.tags.label', ['Sale']),
        ]);
        $this->assertCount(1, $brands);
        $this->assertEquals('brand_x', $brands[0]->getId());

        // "Premium" tag is only on prod_a (BrandX)
        $brands = $database->find('brands', [
            Query::equal('products.tags.label', ['Premium']),
        ]);
        $this->assertCount(1, $brands);
        $this->assertEquals('brand_x', $brands[0]->getId());

        // 2-level deep from the child side: query tags by product→brand name
        $tags = $database->find('tags', [
            Query::equal('products.brands.name', ['BrandY']),
        ]);
        $this->assertCount(1, $tags);
        $this->assertEquals('tag_eco', $tags[0]->getId());

        $tags = $database->find('tags', [
            Query::equal('products.brands.name', ['BrandX']),
        ]);
        $this->assertCount(3, $tags);
        $tagIds = \array_map(fn ($d) => $d->getId(), $tags);
        $this->assertContains('tag_eco', $tagIds);
        $this->assertContains('tag_premium', $tagIds);
        $this->assertContains('tag_sale', $tagIds);

        // No match returns empty
        $brands = $database->find('brands', [
            Query::equal('products.tags.label', ['NonExistent']),
        ]);
        $this->assertCount(0, $brands);

        // Cleanup
        $database->deleteCollection('brands');
        $database->deleteCollection('products');
        $database->deleteCollection('tags');
    }
}
