package com.bc.calvalus.processing.fire;

import org.esa.snap.core.dataio.EncodeQualification;
import org.esa.snap.core.dataio.ProductWriter;
import org.esa.snap.core.dataio.ProductWriterPlugIn;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.util.io.SnapFileFilter;
import org.esa.snap.dataio.netcdf.util.Constants;

import java.io.File;
import java.util.Locale;

public class FrpL3ProductFileWriterPlugIn implements ProductWriterPlugIn {

    @Override
    public EncodeQualification getEncodeQualification(Product product) { return EncodeQualification.FULL; }

    @Override
    public Class[] getOutputTypes() {
        return new Class[]{String.class, File.class};
    }

    @Override
    public ProductWriter createWriterInstance() {
        return new FrpL3ProductFileWriter(this);
    }

    @Override
    public String[] getFormatNames() {
        return new String[] { "NetCDF4-FRP-L3-File" };
    }

    @Override
    public String[] getDefaultFileExtensions() {
        return new String[] { Constants.FILE_EXTENSION_NC };
    }

    @Override
    public String getDescription(Locale locale) {
        return "C3S FRP Level 3 product file";
    }

    @Override
    public SnapFileFilter getProductFileFilter() {
        return new SnapFileFilter(getFormatNames()[0], getDefaultFileExtensions(), getDescription(null));
    }
}
