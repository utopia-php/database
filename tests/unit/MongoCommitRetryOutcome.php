<?php

namespace Tests\Unit;

/**
 * What a scripted commitTransaction does on the server in MongoCommitRetryTest.
 */
enum MongoCommitRetryOutcome
{
    case Committed;

    case AppliedThenLost;

    case NotApplied;

    case Aborted;
}
