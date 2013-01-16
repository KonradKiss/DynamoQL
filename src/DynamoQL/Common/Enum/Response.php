<?php
namespace DynamoQL\Common\Enum;
/**
 * Contains enumerable values used in the framework
 */

use Aws\Common\Enum;

class Response extends Enum
{
    const OK                                    = 200;
    const UNKNOWN                               = 400;

    const ACCESS_DENIED                         = 500;
    const CONDITIONAL_CHECK_FAILED              = 501;
    const DYNAMODB                              = 502;
    const INCOMPLETE_SIGNATURE                  = 503;
    const INTERNAL_FAILURE                      = 504;
    const INTERNAL_SERVER_ERROR                 = 505;
    const LIMIT_EXCEEDED                        = 506;
    const MISSING_AUTHENTICATION_TOKEN          = 507;
    const PROVISIONED_THROUGHPUT_EXCEEDED       = 508;
    const RESOURCE_IN_USE                       = 509;
    const RESOURCE_NOT_FOUND                    = 510;
    const SERVICE_UNAVAILABLE                   = 511;
    const THROTTLING                            = 512;
    const UNPROCESSED_WRITE_REQUESTS            = 513;
    const UNRECOGNIZED_CLIENT                   = 514;
    const VALIDATION                            = 515;
}
