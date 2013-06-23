<?php
namespace DynamoQL\Common\Exception;
/**
 * Created by: Konrad Kiss
 * Date: 1/17/13
 * Time: 7:26 PM
 */
class ExceptionManager
{
    /**
     * @param $e - exception object
     */
    public static function raiseError( \Exception $e )
    {
        error_log($e->getMessage());
        throw $e;
        //trigger_error($e->getMessage(), E_USER_ERROR);
    }

    /**
     * @param \Exception $e               - Exception object (also returned as result['exception'])
     * @param string     $exceptions_ok   - string for a single instance, array for several instances of Exception classes
     *                                    that would be treated as success
     *
     * @return array - a wrapper array containing status, message and exception keys
     */
    public static function process( \Exception $e, $exceptions_ok = "" )
    {
        $resObject = ["status" => \DynamoQL\Common\Enum\Response::OK, "message" => "", "exception" => $e];

        if ( !is_subclass_of($e, "Exception") )
            return $e;

        $className = get_class($e);

        if ( !empty($exceptions_ok) ) {
            if ( gettype($exceptions_ok) == "string" ) {
                $exceptions_ok = [$exceptions_ok];
            }
            if ( gettype($exceptions_ok) == "array" ) {
                if ( in_array($className, $exceptions_ok) )
                    return $resObject; // RESULT_OK, since the error is accepted / expected
            }
        }

        // this is now definitely an exception we don't like

        $resObject['message']        = $e->getMessage();
        $resObject['exception_type'] = $className;

        switch ($className) {
            case "Aws\\DynamoDb\\Exception\\AccessDeniedException":
                $resObject['status'] = \DynamoQL\Common\Enum\Response::ACCESS_DENIED;
                break;
            case "Aws\\DynamoDb\\Exception\\ConditionalCheckFailedException":
                $resObject['status'] = \DynamoQL\Common\Enum\Response::CONDITIONAL_CHECK_FAILED;
                break;
            case "Aws\\DynamoDb\\Exception\\DynamoDbException":
                $resObject['status'] = \DynamoQL\Common\Enum\Response::DYNAMODB;
                break;
            case "Aws\\DynamoDb\\Exception\\IncompleteSignatureException":
                $resObject['status'] = \DynamoQL\Common\Enum\Response::INCOMPLETE_SIGNATURE;
                break;
            case "Aws\\DynamoDb\\Exception\\InternalFailureException":
                $resObject['status'] = \DynamoQL\Common\Enum\Response::INTERNAL_FAILURE;
                break;
            case "Aws\\DynamoDb\\Exception\\InternalServerErrorException":
                $resObject['status'] = \DynamoQL\Common\Enum\Response::INTERNAL_SERVER_ERROR;
                break;
            case "Aws\\DynamoDb\\Exception\\LimitExceededException":
                $resObject['status'] = \DynamoQL\Common\Enum\Response::LIMIT_EXCEEDED;
                break;
            case "MissingAuthenticationTokenException":
                $resObject['status'] = \DynamoQL\Common\Enum\Response::MISSING_AUTHENTICATION_TOKEN;
                break;
            case "Aws\\DynamoDb\\Exception\\ProvisionedThroughputExceededException":
                $resObject['status'] = \DynamoQL\Common\Enum\Response::PROVISIONED_THROUGHPUT_EXCEEDED;
                break;
            case "Aws\\DynamoDb\\Exception\\ResourceInUseException":
                $resObject['status'] = \DynamoQL\Common\Enum\Response::RESOURCE_IN_USE;
                break;
            case "Aws\\DynamoDb\\Exception\\ResourceNotFoundException":
                $resObject['status'] = \DynamoQL\Common\Enum\Response::RESOURCE_NOT_FOUND;
                break;
            case "Aws\\DynamoDb\\Exception\\ServiceUnavailableException":
                $resObject['status'] = \DynamoQL\Common\Enum\Response::SERVICE_UNAVAILABLE;
                break;
            case "Aws\\DynamoDb\\Exception\\ThrottlingException":
                $resObject['status'] = \DynamoQL\Common\Enum\Response::THROTTLING;
                break;
            case "Aws\\DynamoDb\\Exception\\UnprocessedWriteRequestsException":
                $resObject['status'] = \DynamoQL\Common\Enum\Response::UNPROCESSED_WRITE_REQUESTS;
                break;
            case "Aws\\DynamoDb\\Exception\\UnrecognizedClientException":
                $resObject['status'] = \DynamoQL\Common\Enum\Response::UNRECOGNIZED_CLIENT;
                break;
            case "Aws\\DynamoDb\\Exception\\ValidationException":
                $resObject['status'] = \DynamoQL\Common\Enum\Response::VALIDATION;
                break;
            default:
                $resObject['status'] = \DynamoQL\Common\Enum\Response::UNKNOWN;
        }

        return $resObject;
    }
}
