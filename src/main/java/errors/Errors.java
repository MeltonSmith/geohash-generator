package errors;

import com.streamsets.pipeline.api.ErrorCode;

/**
 * Created by: Ian_Rakhmatullin
 * Date: 09.03.2021
 */
public enum Errors implements ErrorCode {
    WRONG_LAT_OR_LONG("Cannot parse either Latitude or Longitude from the record: {}"),
    NO_SUCH_FIELDS_FOR_LAT_OR_LONG("Can't find either {} or {}, among the fields for the record: {}"),
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
