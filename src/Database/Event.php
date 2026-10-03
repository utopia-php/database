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
    case DatabaseDelete = 'database_delete';
    case CollectionList = 'collection_list';
    case CollectionCreate = 'collection_create';
    case CollectionUpdate = 'collection_update';
    case CollectionRead = 'collection_read';
    case CollectionDelete = 'collection_delete';
    case DocumentFind = 'document_find';
    case DocumentPurge = 'document_purge';
    case DocumentCreate = 'document_create';
    case DocumentsCreate = 'documents_create';
    case DocumentRead = 'document_read';
    case DocumentUpdate = 'document_update';
    case DocumentsUpdate = 'documents_update';
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
    case AttributeDelete = 'attribute_delete';
    case IndexRename = 'index_rename';
    case IndexCreate = 'index_create';
    case IndexDelete = 'index_delete';
}
