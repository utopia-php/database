<?php

namespace Utopia\Database;

/**
 * Defines the lifecycle events that can be triggered during database operations.
 */
enum Event: string
{
    case All = '*';
    case DatabaseList = 'database_list';
    case DatabaseCreate = 'database_create';
    case DatabaseUpdate = 'database_update';
    case DatabaseDelete = 'database_delete';
    case CollectionList = 'collection_list';
    case CollectionCreate = 'collection_create';
    case CollectionUpdate = 'collection_update';
    case CollectionRead = 'collection_read';
    case CollectionDelete = 'collection_delete';
    case DocumentFind = 'document_find';
    case DocumentAggregate = 'document_aggregate';
    case DocumentPurge = 'document_purge';
    case DocumentCreate = 'document_create';
    case DocumentsCreate = 'documents_create';
    case DocumentRead = 'document_read';
    case DocumentUpdate = 'document_update';
    case DocumentsUpdate = 'documents_update';
    case DocumentUpsert = 'document_upsert';
    case DocumentsUpsert = 'documents_upsert';
    case DocumentDelete = 'document_delete';
    case DocumentsDelete = 'documents_delete';
    case DocumentCount = 'document_count';
    case DocumentSum = 'document_sum';
    case DocumentIncrease = 'document_increase';
    case DocumentDecrease = 'document_decrease';
    case PermissionsCreate = 'permissions_create';
    case PermissionsRead = 'permissions_read';
    case PermissionsDelete = 'permissions_delete';
    case AttributeCreate = 'attribute_create';
    case AttributesCreate = 'attributes_create';
    case AttributeUpdate = 'attribute_update';
    case AttributeRename = 'attribute_rename';
    case AttributeDelete = 'attribute_delete';
    case IndexRename = 'index_rename';
    case IndexCreate = 'index_create';
    case IndexesCreate = 'indexes_create';
    case IndexDelete = 'index_delete';

    /**
     * The typed event a lifecycle hook receives for this case; All only selects timeouts.
     *
     * @return class-string<Event\Domain>|null
     */
    public function domain(): ?string
    {
        return match ($this) {
            self::All => null,
            self::DatabaseList => Event\Database\Listed::class,
            self::DatabaseCreate => Event\Database\Created::class,
            self::DatabaseUpdate => Event\Database\Updated::class,
            self::DatabaseDelete => Event\Database\Deleted::class,
            self::CollectionList => Event\Collection\Listed::class,
            self::CollectionCreate => Event\Collection\Created::class,
            self::CollectionRead => Event\Collection\Read::class,
            self::CollectionUpdate => Event\Collection\Updated::class,
            self::CollectionDelete => Event\Collection\Deleted::class,
            self::AttributeCreate => Event\Attribute\Created::class,
            self::AttributesCreate => Event\Attribute\BatchCreated::class,
            self::AttributeUpdate => Event\Attribute\Updated::class,
            self::AttributeRename => Event\Attribute\Renamed::class,
            self::AttributeDelete => Event\Attribute\Deleted::class,
            self::IndexCreate => Event\Index\Created::class,
            self::IndexesCreate => Event\Index\BatchCreated::class,
            self::IndexRename => Event\Index\Renamed::class,
            self::IndexDelete => Event\Index\Deleted::class,
            self::DocumentRead => Event\Document\Read::class,
            self::DocumentFind => Event\Document\Found::class,
            self::DocumentAggregate => Event\Document\Aggregated::class,
            self::DocumentCreate => Event\Document\Created::class,
            self::DocumentsCreate => Event\Document\BatchCreated::class,
            self::DocumentUpdate => Event\Document\Updated::class,
            self::DocumentsUpdate => Event\Document\BatchUpdated::class,
            self::DocumentUpsert => Event\Document\Upserted::class,
            self::DocumentsUpsert => Event\Document\BatchUpserted::class,
            self::DocumentDelete => Event\Document\Deleted::class,
            self::DocumentsDelete => Event\Document\BatchDeleted::class,
            self::DocumentIncrease => Event\Document\Increased::class,
            self::DocumentDecrease => Event\Document\Decreased::class,
            self::DocumentCount => Event\Document\Counted::class,
            self::DocumentSum => Event\Document\Summed::class,
            self::DocumentPurge => Event\Document\Purged::class,
            self::PermissionsCreate => Event\Permission\Created::class,
            self::PermissionsRead => Event\Permission\Read::class,
            self::PermissionsDelete => Event\Permission\Deleted::class,
        };
    }
}
