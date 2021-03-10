import ch.hsr.geohash.GeoHash;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneProcessor;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import errors.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.List;

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
    protected List<Stage.ConfigIssue> init() {
        // Validate configuration values and open any required resources.
        List<Stage.ConfigIssue> issues = super.init();

//        if (getConfig().equals("invalidValue")) {
//            issues.add(
//                    getContext().createConfigIssue(
//                            Groups.GEOHASH.name(), "config", Errors.WRONG_CONFIG, "Here's what's wrong..."
//                    )
//            );
//        }

        // If issues is not empty, the UI will inform the user of each configuration issue in the list.
        return issues;
    }

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
//                    LOG.error(new ErrorMessage(Errors.WRONG_LAT_OR_LONG, )record.get("/Id"));
                throw new OnRecordErrorException(record, Errors.WRONG_LAT_OR_LONG, ex);
            }
        }

        //such fields
        if (latitude == null || longitude == null) {
//                LOG.error(Errors.NO_SUCH_FIELDS_FOR_LAT_OR_LONG.getMessage(), record);
            throw new OnRecordErrorException(record, Errors.NO_SUCH_FIELDS_FOR_LAT_OR_LONG, getLatitudePath(), getLongitudePath(), record);
        }

        String hash = GeoHash.geoHashStringWithCharacterPrecision(latitude, longitude, getCharacterPrecision());
        record.set(getGeoHashPath(), Field.create(hash));
        LOG.info("GeoHash generated for the record {}", record);

        batchMaker.addRecord(record);
    }
}
