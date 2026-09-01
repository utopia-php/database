<?php

namespace Utopia\Database;

/**
 * Represents a document change, holding both the old and new versions of a document.
 */
class Change
{
    public function __construct(
        protected Document $old,
        protected Document $new,
    ) {
    }

    /**
     * Get the old document before the change.
     *
     * @return Document
     */
    public function getOld(): Document
    {
        return $this->old;
    }

    /**
     * Set the old document before the change.
     *
     * @param Document $old The previous document state
     * @return void
     */
    public function setOld(Document $old): void
    {
        $this->old = $old;
    }

    /**
     * Get the new document after the change.
     *
     * @return Document
     */
    public function getNew(): Document
    {
        return $this->new;
    }

    /**
     * Set the new document after the change.
     *
     * @param Document $new The updated document state
     * @return void
     */
    public function setNew(Document $new): void
    {
        $this->new = $new;
    }
}
