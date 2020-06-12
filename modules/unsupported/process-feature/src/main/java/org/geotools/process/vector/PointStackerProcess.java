/*
 *    GeoTools - The Open Source Java GIS Toolkit
 *    http://geotools.org
 *
 *    (C) 2011, Open Source Geospatial Foundation (OSGeo)
 *    (C) 2001-2007 TOPP - www.openplans.org.
 *
 *    This library is free software; you can redistribute it and/or
 *    modify it under the terms of the GNU Lesser General Public
 *    License as published by the Free Software Foundation;
 *    version 2.1 of the License.
 *
 *    This library is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *    Lesser General Public License for more details.
 */

package org.geotools.process.vector;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.List;
import java.util.Set;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.logging.Logger;
import de.codematix.bast.PointStackerCM;
import org.geoserver.wps.gs.GeoServerProcess;
import org.geotools.data.collection.ListFeatureCollection;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.process.ProcessException;
import org.geotools.process.factory.DescribeParameter;
import org.geotools.process.factory.DescribeProcess;
import org.geotools.process.factory.DescribeResult;
import org.geotools.referencing.CRS;
import org.geotools.util.factory.FactoryRegistryException;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.impl.PackedCoordinateSequenceFactory;
import org.opengis.feature.Property;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;
import org.opengis.filter.FilterFactory;
import org.opengis.filter.expression.PropertyName;
import org.opengis.filter.sort.SortBy;
import org.opengis.filter.sort.SortOrder;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;
import org.opengis.util.ProgressListener;

/**
 * A Rendering Transformation process which aggregates features into a set of visually
 * non-conflicting point features. The created points have attributes which provide the total number
 * of points aggregated, as well as the number of unique point locations.
 *
 * <p>This is sometimes called "point clustering". The term stacking is used instead, since
 * clustering has multiple meanings in geospatial processing - it is also used to mean identifying
 * groups defined by point proximity.
 *
 * <p>The stacking is defined by specifying a grid to aggregate to. The grid cell size is specified
 * in pixels relative to the requested output image size. This makes it more intuitive to pick an
 * appropriate grid size, and ensures that the aggregation works at all zoom levels.
 *
 * <p>The output is a FeatureCollection containing the following attributes:
 *
 * <ul>
 *   <li><code>geom</code> - the point representing the cluster
 *   <li><code>count</code> - the total number of points in the cluster
 *   <li><code>countunique</code> - the number of unique point locations in the cluster
 * </ul>
 *
 * Note that as required by the Rendering Transformation API, the output has the CRS of the input
 * data.
 *
 * @author mdavis
 * @author Cosmin Cioranu (CozC)
 *
 * *************************************
 * CM_POINTSTACKER (Codematix extensions are marked with "CM_POINTSTACKER [START/END]")
 *
 * <p>(Definition: The data set consists of features that are stacked to cluster. A cluster point then is
 * representing the assigned features. A StackedPoint contains a feature that is stacked to a cluster. Original
 * attributes are column names of the data set connected through the GeoServer datastore. Single points are
 * features that are not assigned to a cluster with other features, thus, count equals 1.)
 *
 * <p> In addition to the original source code and the clustering by grid, our adjustments allow
 * GeoServer users to cluster by an original attribute, such as state or district (depening on the given          
 * data set). The location options 'nearest' and 'weighted' of how to position the cluster point is expanded
 * by 'average' (average of all coordinates of the clustered features) and 'extent' (center of the minimum and maximum
 * coordinates in x and y direction). For this, an additional input parameter clusterBasis is added. Depending on 
 * the use case and the data, these results in more suitable cluster point locations.
 *
 * <p> The output is expanded by additional returning values. The fids (listStackedPointsIDs) and coordinates
 * (listStackedPtsCoos) of the features belonging to the cluster are listed, all available properties 
 * (attributes & values) for single points (singlePointOrigAttributes).
 *
 * <p> If the user wants to sort (sortedByField by overwriting default values of result.SortBy()) the resulting data 
 * collection by a specified original attribute field, for instance grades from 1 to 6, three additional parameters 
 * are introduced: sortField (e.g. 'grade'), sortBy (can be either 'DESCENDING' or 'ASCENDING' (default)), and 
 * clusteredSortValue to set a representative value for cluster points that otherwise would contain a list of grades, 
 * instead of a single number that can be sorted. (we required this sorting method to influence the render order of 
 * the PointSymbolizers in the style .sld file)
 *
 * <p> With these extensions, the user also has the possibility to optionally add original attributes to the output 
 * result (originalAttributes). For instance, if the following list is given in the style as list of original attributes,
 *     ```
 *     <ogc:Function name="parameter">
 *       <ogc:Literal>originalAttributes</ogc:Literal>
 *       <ogc:Literal>attrA,attrB,attrC</ogc:Literal>
 *     </ogc:Function>
 *     ```
 * the extended output is a FeatureCollection containing the following attributes:
 * <ul>
 *    <li><code>fid</code> - the fid of the point representing the cluster 
 *    <li><code>count</code> - the total number of points in the cluster
 *    <li><code>countunique</code> - the number of unique point locations in the cluster
 *    <li><code>envBBOX</code> - bounding box coordinates
 *    <li><code>listStackedPointsIDs</code> - list of all IDs of the stacked features in the cluster
 *    <li><code>listStackedPtsCoos</code> - list of all coordinates of the stacked features in the cluster 
 *    <li><code>singlePointOrigAttributes</code> - only for single points, all of its data properties (attributes & values)
 *    <li><code>sortedByField</code> contains the original attribute values of the sortField, or the representative 
 *                                   argSortValueClusterPt for cluster points, or nothing, if no sortField is indicated 
 *    <li><code>attrA</code> - list of values for the first attribute of the input parameter originalAttributes
 *    <li><code>attrB</code> - list of values for the second attribute of the input parameter originalAttributes
 *    <li><code>attrC</code> - list of values for the third attribute of the input parameter originalAttributes, and so on.
 * </ul>
 *
 * @author Sabrina Arnold sarnold@codematix.de 
 * June 2020
 */

@DescribeProcess(
        title = "Point Stacker",
        description = "Aggregates a collection of points over a grid into one point per grid cell."
)
public class PointStackerProcess implements GeoServerProcess {
    public enum PreserveLocation {
        /** Preserves the original point location in case there is a single point in the cell */
        Single,
        /**
         * Preserves the original point location in case there are multiple points, but all with the
         * same coordinates in the cell
         */
        Superimposed,
        /**
         * Default value, averages the point locations with the cell center to try and avoid
         * conflicts among the symbolizers for the
         */
        Never
    };

    public enum PositionStackedGeom {
        /**  CM_POINTSTACKER: Original calculation of source code: as soon as a point is added to a cluster, the already existing Coo for
         * the stacked clustered point is averaged with the additional point. */
        Weighted,
        /**
         * CM_POINTSTACKER: averages the geometries of all features that are stacked together after all are gathered,
         * cluster is grid-based
         */
        Average, // CM_POINTSTACKER 
        /**
         * CM_POINTSTACKER: average the geometry extent of all features that are stacked together after all are gathered,
         * cluster is grid-based
         */
        Extent, // CM_POINTSTACKER
        /**
         *  CM_POINTSTACKER: Original calculation of source code, default
         */
        Nearest
    };

    public static final String ATTR_GEOM = "geom";

    public static final String ATTR_COUNT = "count";

    public static final String ATTR_COUNT_UNIQUE = "countunique";

    /** bounding box of the clustered points as Poligon Geometry */
    public static final String ATTR_BOUNDING_BOX_GEOM = "geomBBOX";

    /** bounding box of the clustered points as String */
    public static final String ATTR_BOUNDING_BOX = "envBBOX";

    public static final String ATTR_NORM_COUNT = "normCount";

    public static final String ATTR_NORM_COUNT_UNIQUE = "normCountUnique";

    // TODO: add ability to pick index point selection strategy
    // TODO: add ability to set attribute name containing value to be aggregated
    // TODO: add ability to specify aggregation method (COUNT, SUM, AVG)
    // TODO: ultimately could allow aggregating multiple input attributes, with
    // different methods for each
    // TODO: allow including attributes from input data (eg for use with points
    // that are not aggregated)
    // TODO: expand query window to avoid edge effects?

    // no process state is defined, since RenderingTransformation processes must
    // be stateless
    
    // CM_POINTSTACKER START
    // list of coordinates of the stacked points
    public static final String ATTR_STACKED_FEATURES_COO = "listStackedPtsCoos";

    // list of fids of the stacked points
    public static final String ATTR_STACKED_FEATURES_IDS = "listStackedPointsIDs";

    // list of all attributes of a single point (count == 1) 
    public static final String ATTR_SINGLE_PT = "singlePointOrigAttributes";

    // attribute contains the sorted original attribute values of the sortField for single points, or 
    // argSortValueClusterPt for cluster points 
    public static final String ATTR_SORTEDBYFIELD = "sortedByField";
    
    public static org.geotools.process.vector.PointStackerProcess.PositionStackedGeom POSITION_CLUSTER_PT = 
            PositionStackedGeom.Nearest;

    private static final Logger LOG = Logger.getLogger(PointStackerProcess.class.getSimpleName());
    // CM_POINTSTACKER END

    @DescribeResult(name = "result", description = "Aggregated feature collection")
    public SimpleFeatureCollection execute(
            // process data
            @DescribeParameter(name = "data", description = "Input feature collection")
                    SimpleFeatureCollection data,
            // process parameters
            @DescribeParameter(
            // CM_POINTSTACKER START
                    name = "clusterBasis",
                    description = "Indicate basis for clustering methods: 'grid' (default), or an original " +
                            "attribute such as state or district (data-dependent).", 
                    defaultValue = "grid",
                    min = 0
            )
                    String argClusterBasis,
            // CM_POINTSTACKER END
            @DescribeParameter(
                    name = "clusterSize",
                    description = "Size of grid cell to aggregate to. Required for cluster basis 'grid'!", 
                    min = 0 // not necessary, if clusterBasis is an attribute
            )
                    Integer clusterSize,
            @DescribeParameter(
                    name = "positionClusterPt",
                    description = "Weight cluster position based on points added as nearest pt (default), " +
                            "weighted average, total average, or total extent of all stacked coordinates)",
                    defaultValue = "Nearest"
            )
                    PositionStackedGeom argPositionClusterPt,
            @DescribeParameter(
                    name = "normalize",
                    description =  "Indicates whether to add fields normalized to the range 0-1.",
                    defaultValue = "false"
            )
                    Boolean argNormalize,
            @DescribeParameter(
                    // CM_POINTSTACKER START
                    name = "originalAttributes",
                    description = "List of original attributes that are requested to be returned. If these " +
                            "indicated attributes are no actual original attributes of data, they will be ignored. " +
                            "Required as comma-separated list of format, e.g: a,b,c. All in lower case!",
                    min = 0
            )
                    String argOrigAttributes,
            @DescribeParameter(
                    name = "sortField",
                    description = "Attribute field by which resulting collection (optionally) can be sorted.",
                    min = 0
            )
                    String argSortField,
            @DescribeParameter(
                    name = "sortBy",
                    description = "If collection is desired to be sorted through a specified 'sortField', one can " +
                            "indicate the method to sort, either 'DESCENDING' or 'ASCENDING' (default).",
                    min = 0
            )
                    String argSortOrder,
            @DescribeParameter(
                    name = "clusteredSortValue",
                    description = "If a collection is desired to be sorted by a specified column 'sortField', here" +
                            "you can indicate the value that will be used for clustered points as sort value since only" +
                            "single points will keep the original value of the 'sortField' while clustered points " +
                            "gather the values of the 'sortField's to an array which will be replaced by this value " +
                            "instead, such as 0 (default).",
                    min = 0
            )
                    String argSortValueClusterPt,
            // CM_POINTSTACKER END
            @DescribeParameter(
                    name = "preserveLocation",
                    description = "Indicates whether to preserve the original location of points for " +
                            "single/superimposed points",
                    defaultValue = "Never",
                    min = 0
            )
                    PreserveLocation preserveLocation,
            // output image parameters
            @DescribeParameter(
                    name = "outputBBOX",
                    description = "Bounding box for target image extent"
            )
                    ReferencedEnvelope outputEnv,
            @DescribeParameter(
                    name = "outputWidth",
                    description = "Target image width in pixels",
                    minValue = 1
            )
                    Integer outputWidth,
            @DescribeParameter(
                    name = "outputHeight",
                    description = "Target image height in pixels",
                    minValue = 1
            )
                    Integer outputHeight,
            ProgressListener monitor)
            throws ProcessException, TransformException {

        CoordinateReferenceSystem srcCRS = data.getSchema().getCoordinateReferenceSystem();
        CoordinateReferenceSystem dstCRS = outputEnv.getCoordinateReferenceSystem();
        MathTransform crsTransform;
        MathTransform invTransform;
        try {
            crsTransform = CRS.findMathTransform(srcCRS, dstCRS);
            invTransform = crsTransform.inverse();
        } catch (FactoryException e) {
            throw new ProcessException(e);
        }

        boolean normalize = false;
        if (argNormalize != null) {
            normalize = argNormalize;
        }

        // CM_POINTSTACKER START
        POSITION_CLUSTER_PT = argPositionClusterPt;

        // If indicated, initialize attribute to be sorted and representative sort value for clustered points 
        String sortField = null;
        String sortValueClusterPt = "0";
        if (argSortField != null) {
            sortField = argSortField;
        }
        if (argSortValueClusterPt != null) {
            sortValueClusterPt = argSortValueClusterPt;
        }
        
        // get ArrayList of all the original attributes that are requested to be added as column to 
        // the resulting feature info
        ArrayList<String> argOrigAttributesParts = null;
        if (argOrigAttributes != null) {
            argOrigAttributesParts = new ArrayList<>(Arrays.asList(argOrigAttributes.split(",")));
        }

        // if clustering by grid, cluster size is required
        if (clusterSize == null && argClusterBasis.equals("grid")){  
            LOG.warning("NullPointerException! Parameter 'clusterSize' must be indicated for 'clusterBasis' options " +
                    "and 'grid'!");
        }
        // CM_POINTSTACKER END

        double clusterSizeSrc;
        Collection<StackedPoint> stackedPts;

        argClusterBasis = argClusterBasis.toLowerCase(); // CM_POINTSTACKER
        if (argClusterBasis.equals("grid")) {
            // TODO: allow output CRS to be different to data CRS
            // assume same CRS for now...
            clusterSizeSrc = clusterSize * outputEnv.getWidth() / outputWidth;

            // create cluster points, based on clusterSize and width and height of the viewed area.
            stackedPts = StackPointsMethods.stackPointsByGrid(
                    data,
                    argOrigAttributesParts,
                    crsTransform,
                    clusterSizeSrc,
                    sortField,
                    sortValueClusterPt);
        } else {
            // cluster by attribute
            stackedPts = StackPointsMethods.stackPointsByAttribute( // CM_POINTSTACKER (derived from stackPointsByGrid)
                    data,
                    argOrigAttributesParts,
                    crsTransform,
                    argClusterBasis,
                    sortField,
                    sortValueClusterPt);
        }

        SimpleFeatureType schema = createType(srcCRS, normalize, argOrigAttributesParts);
        ListFeatureCollection result = new ListFeatureCollection(schema);
        SimpleFeatureBuilder fb = new SimpleFeatureBuilder(schema);

        GeometryFactory factory = new GeometryFactory(new PackedCoordinateSequenceFactory());

        double[] srcPt = new double[2];
        double[] srcPt2 = new double[2];
        double[] dstPt = new double[2];
        double[] dstPt2 = new double[2];

        // Find maxima of the point stacks if needed.
        int maxCount = 0;
        int maxCountUnique = 0;
        if (normalize) {
            for (StackedPoint sp : stackedPts) {
                if (maxCount < sp.getCount()) maxCount = sp.getCount();
                if (maxCountUnique < sp.getCount()) maxCountUnique = sp.getCountUnique();
            }
        }

        for (StackedPoint sp : stackedPts) {
            // create feature for stacked point
            Coordinate pt = getStackedPointLocation(preserveLocation, sp);

            // transform back to src CRS, since RT rendering expects the output
            // to be in the same CRS
            srcPt[0] = pt.x;
            srcPt[1] = pt.y;
            invTransform.transform(srcPt, 0, dstPt, 0, 1);
            Coordinate psrc = new Coordinate(dstPt[0], dstPt[1]);

            Geometry point = factory.createPoint(psrc);
            fb.add(point);
            fb.add(sp.getCount());
            fb.add(sp.getCountUnique());
            // adding bounding box of the points stacked, as geometry
            // envelope transformation
            Envelope boundingBox = sp.getBoundingBox();
            srcPt[0] = boundingBox.getMinX();
            srcPt[1] = boundingBox.getMinY();
            srcPt2[0] = boundingBox.getMaxX();
            srcPt2[1] = boundingBox.getMaxY();

            invTransform.transform(srcPt, 0, dstPt, 0, 1);
            invTransform.transform(srcPt2, 0, dstPt2, 0, 1);
            Envelope boundingBoxTransformed = new Envelope(dstPt[0], dstPt[1], dstPt2[0], dstPt2[1]);

            fb.add(boundingBoxTransformed);
            // adding bounding box of the points stacked, as string
            fb.add(boundingBoxTransformed.toString());
            if (normalize) {
                fb.add(((double) sp.getCount()) / maxCount);
                fb.add(((double) sp.getCountUnique()) / maxCountUnique);
            }
            // CM_POINTSTACKER START
            fb.add(sp.getListStackedPointsIDs());
            fb.add(sp.getListStackedPointsCoo());
            fb.add(sp.getSinglePointAttr());
            fb.add(sp.getSortField());
            if (argOrigAttributesParts != null) {
                for (String key : argOrigAttributesParts) {
                    fb.add(sp.getOrigAttrMap().get(key)); // get values of each indicated attribute, add as extra column
                }
            }
            // CM_POINTSTACKER END
            result.add(fb.buildFeature(null));
        }

        // CM_POINTSTACKER START
        if (argSortField != null) {
            SimpleFeatureCollection resultSorted = sortCollection(result, PointStackerProcess.ATTR_SORTEDBYFIELD,
                    argSortOrder);
            return resultSorted;
        }
        // CM_POINTSTACKER END
        
        return result;
    }

    /**
     * Extract the geometry depending on the location preservation flag 
     */
    private Coordinate getStackedPointLocation(PreserveLocation preserveLocation,
                                               StackedPoint sp) {
        Coordinate pt = null;
        if (PreserveLocation.Single == preserveLocation) {
            if (sp.getCount() == 1) {
                pt = sp.getOriginalLocation();
            }
        } else if (PreserveLocation.Superimposed == preserveLocation) {
            if (sp.getCountUnique() == 1) {
                pt = sp.getOriginalLocation();
            }
        }
        if (pt == null) {
            pt = sp.getLocation();
        }
        return pt;
    }

    /**
     * CM_POINTSTACKER: Creates a feature type to add the output attributes.
     * @param crs CRS
     * @param stretch normalize
     * @param argOrigAttributesParts list of original attributes that are added as attributes of the output feature type
     * @return SimpleFeatureType
     */
    private SimpleFeatureType createType(CoordinateReferenceSystem crs, boolean stretch,
                                         List<String> argOrigAttributesParts) {
        SimpleFeatureTypeBuilder tb = new SimpleFeatureTypeBuilder();
        tb.add(ATTR_GEOM, Point.class, crs);
        tb.add(ATTR_COUNT, Integer.class);
        tb.add(ATTR_COUNT_UNIQUE, Integer.class);
        tb.add(ATTR_BOUNDING_BOX_GEOM, Polygon.class);
        tb.add(ATTR_BOUNDING_BOX, String.class);
        if (stretch) {
            tb.add(ATTR_NORM_COUNT, Double.class);
            tb.add(ATTR_NORM_COUNT_UNIQUE, Double.class);
        }
        // CM_POINTSTACKER START
        tb.add(ATTR_STACKED_FEATURES_IDS, String.class);
        tb.add(ATTR_STACKED_FEATURES_COO, String.class);
        tb.add(ATTR_SINGLE_PT, Map.class);
        tb.add(ATTR_SORTEDBYFIELD, String.class);
        if (argOrigAttributesParts != null) {
            for (String a: argOrigAttributesParts) {
                tb.add(a, ArrayList.class);
            }
        }
        // CM_POINTSTACKER END
        tb.setName("stackedPoint");
        SimpleFeatureType sfType = tb.buildFeatureType();
        return sfType;
    }


    /**
     * Class for StackedPoint object
     */
    public static class StackedPoint {
        private Coordinate key;

        private Coordinate centerPt;

        private Coordinate location = null;

        private int count = 0;

        private Set<Coordinate> uniquePts;

        private Envelope boundingBox = null;

        // CM_POINTSTACKER START
        private ArrayList<Coordinate> listStackedPtsCoo;

        private ArrayList<String> listStackedPtsIDs;

        private Map<Name, Object> listSinglePtAttributes;
        
        private Collection<Property> featProperties;

        private Map<Name, Object> props = new HashMap<Name, Object>();

        private String sortField = null;
        
        private HashMap<String, ArrayList<String>> origAttr;

        private final Logger LOG = Logger.getLogger(StackedPoint.class.getSimpleName());
        // CM_POINTSTACKER END

        /**
         * Creates a new stacked point grid cell. The center point of the cell is supplied so that
         * it may be used as or influence the location of the final display point
         *
         * @param key a key for the grid cell (using integer ordinates to avoid precision issues)
         * @param centerPt the center point of the grid cell
         */
        public StackedPoint(Coordinate key, Coordinate centerPt) {
            this.key = new Coordinate(key);
            this.centerPt = centerPt;
        }

        public Coordinate getKey() {
            return key;
        }

        /**
         * CM_POINTSTACKER: location of the clustered point depends on the indicated method 
         *                  - average: calculates the average of all x and y coordinates belonging to a cluster 
         *                  - extent: calculates the mean of the minimal and maximal x as well y coordinates
         *                  - weighted/nearest: methods from original source code, calculated before while features are 
         *                  added to their corresponding cluster 
         * @return returns the location where the point indicating a cluster is located 
         */
        public Coordinate getLocation() {
            // CM_POINTSTACKER START
            if (location == null) {
                if (PositionStackedGeom.Average == POSITION_CLUSTER_PT) {
                    location = getAverageAllStackedCoo(listStackedPtsCoo);
                }
                if (PositionStackedGeom.Extent == POSITION_CLUSTER_PT) {
                    location = getExtentAllStackedCoo(listStackedPtsCoo);
                }
                return location;
            }
            // CM_POINTSTACKER END

            return location;
        }

        /**
         * CM_POINTSTACKER:  
         * @return quantity of how many features belong to a cluster 
         */
        public int getCount() {
            return count;
        }

        /**
         * CM_POINTSTACKER:  
         * @return quantity of how many features belong to a cluster  with different coordinates (often features have 
         * identical coordinates)
         */
        public int getCountUnique() {
            if (uniquePts == null) return 1;
            return uniquePts.size();
        }

        // CM_POINTSTACKER START
        /**
         * CM_POINTSTACKER: 
         * @return list of coordinates of the features belonging to a cluster 
         */
        public ArrayList<Coordinate> getListStackedPointsCoo() {
            return listStackedPtsCoo;
        }

        /**
         * CM_POINTSTACKER: 
         * @return list of IDs of the features belonging to a cluster 
         */
        public ArrayList<String> getListStackedPointsIDs() {
            return listStackedPtsIDs;
        }

        /**
         * CM_POINTSTACKER: 
         * @return list of all properties of a single point attribute (= count equals 1)
         */
        public Map<Name, Object> getSinglePointAttr() {
            return listSinglePtAttributes;
        }

        /**
         * CM_POINTSTACKER: project-dependent return value 
         * @return returns the score class ID of a single point attribute (= count equals 1)
         */
        public String getSortField() { return this.sortField; }
        
        /**
         * CM_POINTSTACKER: 
         * @return returns a hashmap with all attributes that are requested through the sld file and their values of the 
         * features belonging to a clustered 
         */
        public HashMap<String, ArrayList<String>> getOrigAttrMap() { return origAttr; }
        // CM_POINTSTACKER END

        /** compute bounding box */
        public Envelope getBoundingBox() {
            return this.boundingBox;
            /*
            Coordinate coords[]=uniquePts.toArray(new Coordinate[uniquePts.size()]);
            Geometry result=factory.createPolygon(coords).getEnvelope();
            System.out.println(result);
            return result;
            */
        }

        /**
         * CM_POINTSTACKER: Adds/stacks a feature point with requested attributes to the corresponding cluster 
         * @param pt Coordinate of the feature that is being added to the StackedPoint, thus a cluster , it belongs to
         * @param id ID of the coordinate that is added to the belonging cluster 
         * @param argOrigAttributesParts list of original attributes that are requested to be returned as 
         *                               feature info in the map
         * @param feature the feature that is being added 
         * @param sortField attribute field to be sorted by
         * @param sortValueClusterPt representative sort value for cluster points
         */
        /** @todo change GeometryFactory */
        public void addPt(Coordinate pt, String id, ArrayList<String> argOrigAttributesParts, SimpleFeature feature , 
                          String sortField, String sortValueClusterPt) {
            /**
             * Count of how many feature points are included within this cluster
             */
            count++;

            // CM_POINTSTACKER START
            /**
             * List of point coordinates that are stacked together
             */
            if (listStackedPtsCoo == null) {
                listStackedPtsCoo = new ArrayList<Coordinate>();
            }
            listStackedPtsCoo.add(pt);

            /**
             * List of IDs of point coordinates that are stacked together
             */
            if (listStackedPtsIDs == null) {
                listStackedPtsIDs = new ArrayList<String>();
            }
            listStackedPtsIDs.add(id);
            // CM_POINTSTACKER END

            /**
             * Get unique points (= different coordinates). Only create set if this is the second point seen (and 
             * assume the first pt is in location)
             */
            if (uniquePts == null) {
                uniquePts = new HashSet<Coordinate>();
            }
            uniquePts.add(pt);

            /**
             * CM_POINTSTACKER: How points are positioned/oriented on map
             */
            if (PositionStackedGeom.Weighted == POSITION_CLUSTER_PT) {
                pickWeightedLocation(pt);
            } else if (PositionStackedGeom.Nearest == POSITION_CLUSTER_PT) {
                pickNearestLocation(pt);
            } else {
                // wait until all coordinates are collected before determining geometry of cluster point
                location = null; // CM_POINTSTACKER
            }

            /**
             * bounding box
             */
            if (boundingBox == null) {
                boundingBox = new Envelope();
            } else {
                boundingBox.expandToInclude(pt);
            }

            // CM_POINTSTACKER START
            /**
             * CM_POINTSTACKER: If count == 1, add all original attributes and their values
             */
            // Get all attribute names and values (to add them if count==1)
            featProperties = feature.getProperties();
            for (Property p : featProperties) {
                props.put(p.getName(), p.getValue());
            }
            if (count == 1) {
                listSinglePtAttributes = props;
            }
            if (count > 1) {
                listSinglePtAttributes = null;
            }

            /**
             * CM_POINTSTACKER: For single point features, add the sort attribute, and use representative value of 
             * ATTR_CLUSTERPT_SORT_VALUE for all clustered points since otherwise their attribute values would be 
             * arranged in a list which cannot be sorted
             */
            if (this.count == 1) { // only add these feature info only for single points
                try {
                    this.sortField = feature.getAttribute(sortField).toString();
                }
                catch (NullPointerException e) {
                    this.sortField = null;
                    LOG.warning(e.getMessage() + " Failed to load indicated sortField for this features!");
                }
            } else if (this.count == 2) { // if more than a single point, set back to default value for cluster points
                this.sortField = sortValueClusterPt;
            }
            
            /**
             * CM_POINTSTACKER: Get all requested original attributes as additional column fields 
             */
            if (argOrigAttributesParts != null) {
                if (origAttr == null) {
                    origAttr = new HashMap<String, ArrayList<String>>();
                }
                origAttr = getRequestedOrigAttr(origAttr, argOrigAttributesParts, feature);
            }
            // CM_POINTSTACKER END
        }

        /**
         * The original location of the points, in case they are all superimposed (or there is a
         * single point), otherwise null
         */
        public Coordinate getOriginalLocation() {
            if (uniquePts != null && uniquePts.size() == 1) {
                return uniquePts.iterator().next();
            } else {
                return null;
            }
        }

        /** Calculate the weighted position of the cluster based on points which it holds. */
        private void pickWeightedLocation(Coordinate pt) {
            if (location == null) {
                location = pt;
                return;
            }

            location = weightedAverage(location, pt);
        }

        /**
         * CM_POINTSTACKER: Calculate position of point representing clustered features by averaging coordinates of 
         * total amount of clustered features within the grid cell. 
         * @return coordinate of location where cluster point is positioned
         */
        private Coordinate getAverageAllStackedCoo(ArrayList<Coordinate> allStackedCoordinates) {
            // CM_POINTSTACKER START
            double sumX = 0;
            double sumY = 0;
            int amountStackedFeatures = allStackedCoordinates.size();

            for (int i = 0; i < amountStackedFeatures; i++) {
                Coordinate p = allStackedCoordinates.get(i);
                sumX = sumX + p.x;
                sumY = sumY + p.y;
            }
            double avgX = sumX / amountStackedFeatures;
            double avgY = sumY / amountStackedFeatures;

            return new Coordinate(avgX, avgY);
            // CM_POINTSTACKER END
        }

        /**
         * CM_POINTSTACKER: Calculate position of point representing clustered feature by the average of the extents of 
         * the stacked features within the grid cell. 
         * @return coordinate of location where cluster point is positioned
         */
        private Coordinate getExtentAllStackedCoo(ArrayList<Coordinate> allStackedCoordinates) {
            // CM_POINTSTACKER START
            // initialize min/max extent values with first feature coordinates
            Coordinate p1 = allStackedCoordinates.get(0);
            double min_extentX = p1.x;;
            double max_extentX = p1.x;
            double min_extentY = p1.y;
            double max_extentY = p1.y;
            int amountStackedFeatures = allStackedCoordinates.size();

            for (int i = 0; i < amountStackedFeatures; i++) {
                Coordinate p = allStackedCoordinates.get(i);
                if (p.x < min_extentX) { min_extentX = p.x; }
                if (p.x > max_extentX) { max_extentX = p.x; }
                if (p.y < min_extentY) { min_extentY = p.y; }
                if (p.y > max_extentY) { max_extentY = p.y; }
            }
            double x = (max_extentX + min_extentX) / 2;
            double y = (max_extentY + min_extentY) / 2;

            return new Coordinate(x, y);
            // CM_POINTSTACKER END
        }

        /**
         * Picks the location as the point which is nearest to the center of the cell. In addition,
         * the nearest location is averaged with the cell center. This gives the best chance of
         * avoiding conflicts.
         */
        private void pickNearestLocation(Coordinate pt) {
            // strategy - pick most central point
            if (location == null) {
                location = weightedAverage(centerPt, pt);
                return;
            }
            if (pt.distance(centerPt) < location.distance(centerPt)) {
                location = weightedAverage(centerPt, pt);
            }
        }

        /**
         * CM_POINTSTACKER: calculates the mean of two coordinates
         * @param p1 coordinate of the points representing the a cluster , potentially a previous mean coordinate
         * @param p2 coordinate of the feature that is being added to the cluster 
         * @return Coordinate as calculated mean of p1 and p2
         */
        private Coordinate weightedAverage(Coordinate p1, Coordinate p2) {
            double x = (p1.x + p2.x) / 2;
            double y = (p1.y + p2.y) / 2;
            return new Coordinate(x, y);
        }

        /**
         * CM_POINTSTACKER: within the sld file of the GeoServer style, a list of original attributes can be indicated 
         * by the user to return their values of the features that belong to a clustered 
         * @param origAttr hashmap that is filled with requested attributes and their values 
         * @param argOrigAttributesParts list of requested original attributes through the sld file
         * @param feature the feature that is being added to a cluster  
         * @return hashmap of requested attributes (key) and their values 
         */
        private HashMap<String, ArrayList<String>> getRequestedOrigAttr(
                HashMap<String, ArrayList<String>> origAttr, ArrayList<String> argOrigAttributesParts, SimpleFeature feature)
        {
            String attrValue;
            ArrayList<String> fieldValues;

            for (String a : argOrigAttributesParts) {
                if (!origAttr.containsKey(a)) {
                    origAttr.put(a, new ArrayList<String>());
                }
                fieldValues = origAttr.get(a);

                // Check if indicated field name is actually original attribute name#
                try {
                    attrValue = feature.getAttribute(a).toString();
                    fieldValues.add(attrValue);
                    origAttr.put(a, fieldValues);
                } catch (java.lang.NullPointerException e) {
                    LOG.warning(e.getMessage() + " Failed to load indicated attribute " + a);
                }
            }

            return origAttr;
        }
    }
    
    
    /**
     * Class for available clustering methods: cluster by grid or by attribute
     */
    public static class StackPointsMethods {
        // CM_POINTSTACKER START
        private static final Logger LOG = Logger.getLogger(StackPointsMethods.class.getSimpleName());
        // CM_POINTSTACKER END
    
        /**
         * Computes the stacked points for the given data collection. All geometry types are handled -
         * for non-point geometries, the centroid is used.
         *
         * CM_POINTSTACKER: This method creates a grid over the map, and assigns the features according to which grid 
         * cell they belong to 
         * @param data data set
         * @param argOrigAttributesParts list of attributes and their values that are requested to be returned
         * @param crsTransform CRS transformation
         * @param clusterSize size of the grid cells 
         * @return collection of features (StackedPoint) that are stacked together to a cluster group
         */
        public static Collection<StackedPoint> stackPointsByGrid(
                SimpleFeatureCollection data,
                ArrayList<String> argOrigAttributesParts, // CM_POINTSTACKER 
                MathTransform crsTransform,
                double clusterSize,
                String sortField,
                String sortValueClusterPt)
                throws TransformException {
    
            SimpleFeatureIterator featureIt = data.features();
            Map<Coordinate, StackedPoint> stackedPts = new HashMap<Coordinate, StackedPoint>();
            double[] srcPt = new double[2];
            double[] dstPt = new double[2];
            Coordinate indexPt = new Coordinate();
            String id;
            Collection<Property> featureProperties;
            Map<Name, Object> dictProperties;
    
            try {
                while (featureIt.hasNext()) {
                    SimpleFeature feature = featureIt.next();
    
                    // CM_POINTSTACKER START
                    // get ID of current feature
                    id = feature.getID();
    
                    // Get all attribute names and values (to add them if single point, thus, count==1)
                    featureProperties = feature.getProperties();
                    dictProperties = new HashMap<Name, Object>();
                    for (Property p : featureProperties) {
                        dictProperties.put(p.getName(), p.getValue());
                    }
                    // CM_POINTSTACKER END
    
                    // get the point location from the geometry
                    Geometry geom = (Geometry) feature.getDefaultGeometry();
                    Coordinate p = StackPointsMethods.getRepresentativePoint(geom);
    
                    // reproject data point to output CRS, if required
                    srcPt[0] = p.x;
                    srcPt[1] = p.y;
                    crsTransform.transform(srcPt, 0, dstPt, 0, 1);
                    Coordinate pout = new Coordinate(dstPt[0], dstPt[1]);
    
                    indexPt.x = pout.x;
                    indexPt.y = pout.y;
                    StackPointsMethods.gridIndex(indexPt, clusterSize);
    
                    StackedPoint stkPt = stackedPts.get(indexPt);
                    if (stkPt == null) {
                        // Note that we compute the cluster position in the middle of the grid 
                        double centreX = indexPt.x * clusterSize + clusterSize / 2;
                        double centreY = indexPt.y * clusterSize + clusterSize / 2;
    
                        stkPt = new StackedPoint(indexPt, new Coordinate(centreX, centreY));
                        // stkPt.addInitialPoint(pout);
                        stackedPts.put(stkPt.getKey(), stkPt);
                    }
    
                    stkPt.addPt(pout, id, argOrigAttributesParts, feature, sortField, sortValueClusterPt);
                }
            } finally {
                featureIt.close();
            }
    
            return stackedPts.values();
        }
    
        /**
         * CM_POINTSTACKER: This method clusters features by a user-indicated attribute in the sld file, 
         * such as state or district.
         * @param data data set
         * @param argOrigAttributesParts list of attributes and their values that are requested to be returned
         * @param crsTransform CRS transformation
         * @param argClusterByAttribute the user-indicated attribute after which is being stacked into cluster groups      
         * @return collection of features (StackedPoint) that are stacked together to a cluster group
         */
        public static Collection<StackedPoint> stackPointsByAttribute(
                SimpleFeatureCollection data,
                ArrayList<String> argOrigAttributesParts, // CM_POINTSTACKER 
                MathTransform crsTransform,
                String argClusterByAttribute,
                String sortField,
                String sortValueClusterPt)
                throws TransformException {
    
            SimpleFeatureIterator featureIt = data.features();
    
            double[] srcPt = new double[2];
            double[] dstPt = new double[2];
    
            Coordinate indexPt = new Coordinate();
    
            String id;
    
            Collection<Property> featureProperties;
            Map<Name, Object> dictProperties;
    
            // CM_POINTSTACKER START
            Map<String, StackedPoint> stackedPts = new HashMap<String, StackedPoint>();
    
            String clusterAttr;
            // CM_POINTSTACKER END
    
            try {
                first:
                while (featureIt.hasNext()) {
                    SimpleFeature feature = featureIt.next();
    
                    // CM_POINTSTACKER START
                    // get ID of current feature
                    id = feature.getID();
    
                    // Get all attribute names and values (to add them if count==1)
                    featureProperties = feature.getProperties();
                    dictProperties = new HashMap<Name, Object>();
                    for (Property p : featureProperties) {
                        dictProperties.put(p.getName(), p.getValue());
                    }
                    // CM_POINTSTACKER END
    
                    // get the point location from the geometry
                    Geometry geom = (Geometry) feature.getDefaultGeometry();
                    Coordinate p = StackPointsMethods.getRepresentativePoint(geom);
    
                    // reproject data point to output CRS, if required
                    srcPt[0] = p.x;
                    srcPt[1] = p.y;
                    crsTransform.transform(srcPt, 0, dstPt, 0, 1);
                    Coordinate pout = new Coordinate(dstPt[0], dstPt[1]);
    
                    // CM_POINTSTACKER START
                    // Features that do not have a value for the given attribute are not clustered
                    try {
                        clusterAttr = feature.getAttribute(argClusterByAttribute).toString();
                    } catch (NullPointerException e) {
                        LOG.warning(
                                e.getMessage() + " This feature does not contain any value for " + argClusterByAttribute);
                        continue first;
                    }
    
                    StackedPoint stkPt = stackedPts.get(clusterAttr);
                    if (stkPt == null) {
                        stkPt = new StackedPoint(indexPt, new Coordinate(pout.x, pout.y));
                        stackedPts.put(clusterAttr, stkPt);
                    }
                    // CM_POINTSTACKER END
    
                    stkPt.addPt(pout, id, argOrigAttributesParts, feature, sortField, sortValueClusterPt);
                }
            } finally {
                featureIt.close();
            }
    
            return stackedPts.values();
        }
        
        /**
         * CM_POINTSTACKER: maps the feature coordinates to the lower left grid cell corner (by rounding through long 
         * data type) to stack all features to a cluster group that are mapped to the same corner
         *
         * Computes the grid index for a point for the grid determined by the clusterSize.
         *
         * @param griddedPt the point to grid, and also holds the output value
         * @param clusterSize the grid cell size
         */
        public static void gridIndex(Coordinate griddedPt, double clusterSize) {
            // TODO: is there any situation where this could result in too much loss
            // of precision?
            /**
             * The grid is based at the origin of the entire data space, not just the query window. This
             * makes gridding stable during panning.
             *
             * <p>This should not lose too much precision for any reasonable coordinate system and map
             * size. The worst case is a CRS with small ordinate values, and a large cell size. The
             * worst case tested is a map in degrees, zoomed out to show about twice the globe - works
             * fine.
             */
            // Use longs to avoid possible overflow issues (e.g. for a very small cell size)
            long ix = (long) ((griddedPt.x) / clusterSize);
            long iy = (long) ((griddedPt.y) / clusterSize);
    
            griddedPt.x = ix;
            griddedPt.y = iy;
        }
    
        /**
         * Gets a point to represent the Geometry. If the Geometry is a point, this is returned.
         * Otherwise, the centroid is used.
         *
         * @param g the geometry to find a point for
         * @return a point representing the Geometry
         */
        public static Coordinate getRepresentativePoint(Geometry g) {
            if (g.getNumPoints() == 1) return g.getCoordinate();
            return g.getCentroid().getCoordinate();
        }
    }

    /**
     * CM_POINTSTACKER: Sort the resulted clustered collection  
     * @param result SimpleFeatureCollection to be sorted
     * @param argSortOrder DESCENDING or ASCENDING 
     * @return
     */
    private SimpleFeatureCollection sortCollection(SimpleFeatureCollection result, String sortBy, String argSortOrder) {
        // CM_POINTSTACKER START
        final String sortByParameter = sortBy;
        final String sortOrder = argSortOrder;
        SimpleFeatureCollection resultSorted = result.sort( new SortBy()
        {
            @Override
            public PropertyName getPropertyName() {
                FilterFactory ff = null;
                try {
                    ff = CommonFactoryFinder.getFilterFactory( null );
                } catch (FactoryRegistryException e) {
                    e.printStackTrace();
                }
                PropertyName propertyName = ff.property(sortByParameter);
                return propertyName;
            }
            @Override
            public SortOrder getSortOrder() {
                if (sortOrder == "DESCENDING") { return SortOrder.DESCENDING; }
                return SortOrder.ASCENDING;
            }
        });
        return resultSorted;
        // CM_POINTSTACKER END
    }
    
}
