import ch.hsr.geohash.GeoHash;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneProcessor;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import errors.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by: Ian_Rakhmatullin
 * Date: 09.03.2021
 */
public abstract class GeoHashProcessor extends SingleLaneRecordProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(GeoHashProcessor.class);

    /**
     * Gives access to the UI configurations of the stage provided by the {@link GeoHashDProcessor} class.
     */
    public abstract int getCharacterPrecision();
    public abstract String getLatitudePath();
    public abstract String getGeoHashPath();
    public abstract String getLongitudePath();


    /** {@inheritDoc} */
    @Override
    public void destroy() {
        // Clean up any open resources.
        super.destroy();
    }

    /** {@inheritDoc} */
    @Override
    protected void process(Record record, SingleLaneProcessor.SingleLaneBatchMaker batchMaker) throws StageException {
        Double latitude = null;
        Double longitude = null;

        //seek for lat and long
        for (String fieldPath : record.getEscapedFieldPaths()) {
            try {
                if (fieldPath.equals(getLatitudePath()))
                    latitude = record.get(fieldPath).getValueAsDouble();
                else if (fieldPath.equals(getLongitudePath())) {
                        longitude = record.get(fieldPath).getValueAsDouble();
                }
            } catch (IllegalArgumentException | NullPointerException ex) {
                OnRecordErrorException onRecordErrorException = new OnRecordErrorException(record, Errors.WRONG_LAT_OR_LONG, record, ex);
                LOG.error(onRecordErrorException.getMessage());
                throw onRecordErrorException;
            }
        }

        //no such fields
        if (latitude == null || longitude == null) {
            OnRecordErrorException onRecordErrorException = new OnRecordErrorException(record, Errors.NO_SUCH_FIELDS_FOR_LAT_OR_LONG, getLatitudePath(), getLongitudePath(), record);
            LOG.error(onRecordErrorException.getMessage());
            throw onRecordErrorException;
        }

        String hash = GeoHash.geoHashStringWithCharacterPrecision(latitude, longitude, getCharacterPrecision());
        record.set(getGeoHashPath(), Field.create(hash));
        LOG.info("GeoHash generated for the record {}", record);

        batchMaker.addRecord(record);
    }
}
