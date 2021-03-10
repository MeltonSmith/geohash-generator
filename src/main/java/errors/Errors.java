package errors;

import com.streamsets.pipeline.api.ErrorCode;

/**
 * Created by: Ian_Rakhmatullin
 * Date: 09.03.2021
 */
public enum Errors implements ErrorCode {
    WRONG_FORMAT("Cannot parse a number from the field {} of the record: {}"),
    NO_SUCH_FIELD("Can't find {}, among the fields of the record: {}"),
    ;
    private final String msg;

    Errors(String msg) {
        this.msg = msg;
    }

    /** {@inheritDoc} */
    @Override
    public String getCode() {
        return name();
    }

    /** {@inheritDoc} */
    @Override
    public String getMessage() {
        return msg;
    }
}
