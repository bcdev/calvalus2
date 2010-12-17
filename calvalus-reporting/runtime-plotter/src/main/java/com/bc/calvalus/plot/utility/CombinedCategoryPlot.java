package com.bc.calvalus.plot.utility;

import org.jfree.chart.LegendItemCollection;
import org.jfree.chart.axis.AxisSpace;
import org.jfree.chart.axis.AxisState;
import org.jfree.chart.axis.CategoryAxis;
import org.jfree.chart.axis.ValueAxis;
import org.jfree.chart.event.PlotChangeEvent;
import org.jfree.chart.event.PlotChangeListener;
import org.jfree.chart.plot.*;
import org.jfree.data.Range;
import org.jfree.ui.RectangleEdge;
import org.jfree.ui.RectangleInsets;
import org.jfree.util.ObjectUtilities;

import java.awt.*;
import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;


public class CombinedCategoryPlot extends CategoryPlot implements PlotChangeListener {

    private List<CategoryPlot> subplots;
    private double gap; //The gap between subplots.
    private transient Rectangle2D[] subplotAreas;  //Temporary storage for the subplot areas.

    /**
     * Default constructor.
     */
    public CombinedCategoryPlot() {
        this(new CategoryAxis());
    }

    /**
     * Creates a new plot.
     *
     * @param domainAxis the shared domain axis.
     */
    public CombinedCategoryPlot(CategoryAxis domainAxis) {
        super(null, domainAxis, null, null);
        this.subplots = new java.util.ArrayList();
        this.gap = 5.0;
    }

    public double getGap() {
        return this.gap;
    }

    /**
     * Sets the amount of space between subplots and sends a
     * {@link PlotChangeEvent} to all registered listeners.
     *
     * @param gap the gap between subplots (in Java2D units).
     */
    public void setGap(double gap) {
        this.gap = gap;
        fireChangeEvent();
    }

    public void add(CategoryPlot subplot) {
        add(subplot, 1);
    }

    public void add(CategoryPlot subplot, int weight) {
        if (subplot == null) {
            throw new IllegalArgumentException("Null 'subplot' argument.");
        }
        if (weight < 1) {
            throw new IllegalArgumentException("Require weight >= 1.");
        }
        subplot.setParent(this);
        subplot.setWeight(weight);
        subplot.setInsets(new RectangleInsets(0.0, 0.0, 0.0, 0.0));
        subplot.setOrientation(getOrientation());
        subplot.addChangeListener(this);
        subplots.add(subplot);
        fireChangeEvent();
    }

    /**
     * Removes a subplot from the combined chart.  Potentially, this removes
     * some unique categories from the overall union of the datasets...so the
     * domain axis is reconfigured, then a {@link PlotChangeEvent} is sent to
     * all registered listeners.
     *
     * @param subplot the subplot (<code>null</code> not permitted).
     */
    public void remove(CategoryPlot subplot) {
        if (subplot == null) {
            throw new IllegalArgumentException("Null 'subplot' argument.");
        }
        int position = -1;
        int size = this.subplots.size();
        int i = 0;
        while (position == -1 && i < size) {
            if (this.subplots.get(i) == subplot) {
                position = i;
            }
            i++;
        }
        if (position != -1) {
            this.subplots.remove(position);
            subplot.setParent(null);
            subplot.removeChangeListener(this);
            CategoryAxis domain = getDomainAxis();
            if (domain != null) {
                domain.configure();
            }
            fireChangeEvent();
        }
    }

    public List getSubplots() {
        if (this.subplots != null) {
            return Collections.unmodifiableList(this.subplots);
        } else {
            return Collections.EMPTY_LIST;
        }
    }

    /**
     * Returns the subplot (if any) that contains the (x, y) point (specified
     * in Java2D space).
     *
     * @param info   the chart rendering info (<code>null</code> not permitted).
     * @param source the source point (<code>null</code> not permitted).
     * @return A subplot (possibly <code>null</code>).
     */
    public CategoryPlot findSubplot(PlotRenderingInfo info, Point2D source) {
        if (info == null) {
            throw new IllegalArgumentException("Null 'info' argument.");
        }
        if (source == null) {
            throw new IllegalArgumentException("Null 'source' argument.");
        }
        CategoryPlot result = null;
        int subplotIndex = info.getSubplotIndex(source);
        if (subplotIndex >= 0) {
            result = subplots.get(subplotIndex);
        }
        return result;
    }

    /**
     * Multiplies the range on the range axis/axes by the specified factor.
     *
     * @param factor the zoom factor.
     * @param info   the plot rendering info (<code>null</code> not permitted).
     * @param source the source point (<code>null</code> not permitted).
     */
    public void zoomRangeAxes(double factor, PlotRenderingInfo info, Point2D source) {
        zoomRangeAxes(factor, info, source, false);
    }

    /**
     * Multiplies the range on the range axis/axes by the specified factor.
     *
     * @param factor    the zoom factor.
     * @param info      the plot rendering info (<code>null</code> not permitted).
     * @param source    the source point (<code>null</code> not permitted).
     * @param useAnchor zoom about the anchor point?
     */
    public void zoomRangeAxes(double factor, PlotRenderingInfo info, Point2D source, boolean useAnchor) {
        // delegate 'info' and 'source' argument checks...
        CategoryPlot subplot = findSubplot(info, source);
        if (subplot != null) {
            subplot.zoomRangeAxes(factor, info, source, useAnchor);
        } else {
            // if the source point doesn't fall within a subplot, we do the
            // zoom on all subplots...
            Iterator iterator = getSubplots().iterator();
            while (iterator.hasNext()) {
                subplot = (CategoryPlot) iterator.next();
                subplot.zoomRangeAxes(factor, info, source, useAnchor);
            }
        }
    }

    /**
     * Zooms in on the range axes.
     *
     * @param lowerPercent the lower bound.
     * @param upperPercent the upper bound.
     * @param info         the plot rendering info (<code>null</code> not permitted).
     * @param source       the source point (<code>null</code> not permitted).
     */
    public void zoomRangeAxes(double lowerPercent, double upperPercent, PlotRenderingInfo info, Point2D source) {
        // delegate 'info' and 'source' argument checks...
        CategoryPlot subplot = findSubplot(info, source);
        if (subplot != null) {
            subplot.zoomRangeAxes(lowerPercent, upperPercent, info, source);
        } else {
            // if the source point doesn't fall within a subplot, we do the
            // zoom on all subplots...
            Iterator iterator = getSubplots().iterator();
            while (iterator.hasNext()) {
                subplot = (CategoryPlot) iterator.next();
                subplot.zoomRangeAxes(lowerPercent, upperPercent, info, source);
            }
        }
    }

    /**
     * Calculates the space required for the axes.
     *
     * @param g2       the graphics device.
     * @param plotArea the plot area.
     * @return The space required for the axes.
     */
    protected AxisSpace calculateAxisSpace(Graphics2D g2, Rectangle2D plotArea) {

        AxisSpace space = new AxisSpace();
        PlotOrientation orientation = getOrientation();

        // work out the space required by the domain axis...
        AxisSpace fixed = getFixedDomainAxisSpace();
        if (fixed != null) {
            if (orientation == PlotOrientation.HORIZONTAL) {
                space.setLeft(fixed.getLeft());
                space.setRight(fixed.getRight());
            } else if (orientation == PlotOrientation.VERTICAL) {
                space.setTop(fixed.getTop());
                space.setBottom(fixed.getBottom());
            }
        } else {
            CategoryAxis categoryAxis = getDomainAxis();
            RectangleEdge categoryEdge = Plot.resolveDomainAxisLocation(
                    getDomainAxisLocation(), orientation);
            if (categoryAxis != null) {
                space = categoryAxis.reserveSpace(g2, this, plotArea,
                                                  categoryEdge, space);
            } else {
                if (getDrawSharedDomainAxis()) {
                    space = getDomainAxis().reserveSpace(g2, this, plotArea,
                                                         categoryEdge, space);
                }
            }
        }

        Rectangle2D adjustedPlotArea = space.shrink(plotArea, null);

        // work out the maximum height or width of the non-shared axes...
        int n = this.subplots.size();
        int totalWeight = 0;
        for (int i = 0; i < n; i++) {
            CategoryPlot sub = (CategoryPlot) this.subplots.get(i);
            totalWeight += sub.getWeight();
        }
        this.subplotAreas = new Rectangle2D[n];
        double x = adjustedPlotArea.getX();
        double y = adjustedPlotArea.getY();
        double usableSize = 0.0;
        if (orientation == PlotOrientation.HORIZONTAL) {
            usableSize = adjustedPlotArea.getWidth() - this.gap * (n - 1);
        } else if (orientation == PlotOrientation.VERTICAL) {
            usableSize = adjustedPlotArea.getHeight() - this.gap * (n - 1);
        }

        for (int i = 0; i < n; i++) {
            CategoryPlot plot = (CategoryPlot) this.subplots.get(i);

            // calculate sub-plot area
            if (orientation == PlotOrientation.HORIZONTAL) {
                double w = usableSize * plot.getWeight() / totalWeight;
                this.subplotAreas[i] = new Rectangle2D.Double(x, y, w,
                                                              adjustedPlotArea.getHeight());
                x = x + w + this.gap;
            } else if (orientation == PlotOrientation.VERTICAL) {
                double h = usableSize * plot.getWeight() / totalWeight;
                this.subplotAreas[i] = new Rectangle2D.Double(x, y,
                                                              adjustedPlotArea.getWidth(), h);
                y = y + h + this.gap;
            }

//            AxisSpace subSpace = plot.calculateRangeAxisSpace(g2, this.subplotAreas[i], null);
            final AxisSpace subSpace = new AxisSpace();
            space.ensureAtLeast(subSpace);

        }

        return space;
    }

    /**
     * Draws the plot on a Java 2D graphics device (such as the screen or a
     * printer).  Will perform all the placement calculations for each of the
     * sub-plots and then tell these to draw themselves.
     *
     * @param g2          the graphics device.
     * @param area        the area within which the plot (including axis labels)
     *                    should be drawn.
     * @param anchor      the anchor point (<code>null</code> permitted).
     * @param parentState the state from the parent plot, if there is one.
     * @param info        collects information about the drawing (<code>null</code>
     *                    permitted).
     */
    public void draw(Graphics2D g2,
                     Rectangle2D area,
                     Point2D anchor,
                     PlotState parentState,
                     PlotRenderingInfo info) {

        // set up info collection...
        if (info != null) {
            info.setPlotArea(area);
        }

        // adjust the drawing area for plot insets (if any)...
        RectangleInsets insets = getInsets();
        area.setRect(area.getX() + insets.getLeft(),
                     area.getY() + insets.getTop(),
                     area.getWidth() - insets.getLeft() - insets.getRight(),
                     area.getHeight() - insets.getTop() - insets.getBottom());


        // calculate the data area...
        setFixedRangeAxisSpaceForSubplots(null);
        AxisSpace space = calculateAxisSpace(g2, area);
        Rectangle2D dataArea = space.shrink(area, null);

        // set the width and height of non-shared axis of all sub-plots
        setFixedRangeAxisSpaceForSubplots(space);

        // draw the shared axis
        CategoryAxis axis = getDomainAxis();
        RectangleEdge domainEdge = getDomainAxisEdge();
        double cursor = RectangleEdge.coordinate(dataArea, domainEdge);
        AxisState axisState = axis.draw(g2, cursor, area, dataArea,
                                        domainEdge, info);
        if (parentState == null) {
            parentState = new PlotState();
        }
        parentState.getSharedAxisStates().put(axis, axisState);

        // draw all the subplots
        for (int i = 0; i < this.subplots.size(); i++) {
            CategoryPlot plot = (CategoryPlot) this.subplots.get(i);
            PlotRenderingInfo subplotInfo = null;
            if (info != null) {
                subplotInfo = new PlotRenderingInfo(info.getOwner());
                info.addSubplotInfo(subplotInfo);
            }
            Point2D subAnchor = null;
            if (anchor != null && this.subplotAreas[i].contains(anchor)) {
                subAnchor = anchor;
            }
            plot.draw(g2, this.subplotAreas[i], subAnchor, parentState,
                      subplotInfo);
        }

        if (info != null) {
            info.setDataArea(dataArea);
        }

    }

    /**
     * Sets the size (width or height, depending on the orientation of the
     * plot) for the range axis of each subplot.
     *
     * @param space the space (<code>null</code> permitted).
     */
    protected void setFixedRangeAxisSpaceForSubplots(AxisSpace space) {
        Iterator iterator = this.subplots.iterator();
        while (iterator.hasNext()) {
            CategoryPlot plot = (CategoryPlot) iterator.next();
            plot.setFixedRangeAxisSpace(space, false);
        }
    }

    /**
     * Sets the orientation of the plot (and all subplots).
     *
     * @param orientation the orientation (<code>null</code> not permitted).
     */
    public void setOrientation(PlotOrientation orientation) {

        super.setOrientation(orientation);

        Iterator iterator = this.subplots.iterator();
        while (iterator.hasNext()) {
            CategoryPlot plot = (CategoryPlot) iterator.next();
            plot.setOrientation(orientation);
        }

    }

    /**
     * Returns a range representing the extent of the data values in this plot
     * (obtained from the subplots) that will be rendered against the specified
     * axis.  NOTE: This method is intended for internal JFreeChart use, and
     * is public only so that code in the axis classes can call it.  Since,
     * for this class, the domain axis is a {@link CategoryAxis}
     * (not a <code>ValueAxis</code}) and subplots have independent range axes,
     * the JFreeChart code will never call this method (although this is not
     * checked/enforced).
     *
     * @param axis the axis.
     * @return The range.
     */
    public Range getDataRange(ValueAxis axis) {
        // override is only for documentation purposes
        return super.getDataRange(axis);
    }

    /**
     * Returns a collection of legend items for the plot.
     *
     * @return The legend items.
     */
    public LegendItemCollection getLegendItems() {
        LegendItemCollection result = getFixedLegendItems();
        if (result == null) {
            result = new LegendItemCollection();
            if (this.subplots != null) {
                Iterator iterator = this.subplots.iterator();
                while (iterator.hasNext()) {
                    CategoryPlot plot = (CategoryPlot) iterator.next();
                    LegendItemCollection more = plot.getLegendItems();
                    result.addAll(more);
                }
            }
        }
        return result;
    }

    /**
     * Returns an unmodifiable list of the categories contained in all the
     * subplots.
     *
     * @return The list.
     */
    public List getCategories() {
        List result = new java.util.ArrayList();
        if (this.subplots != null) {
            Iterator iterator = this.subplots.iterator();
            while (iterator.hasNext()) {
                CategoryPlot plot = (CategoryPlot) iterator.next();
                List more = plot.getCategories();
                Iterator moreIterator = more.iterator();
                while (moreIterator.hasNext()) {
                    Comparable category = (Comparable) moreIterator.next();
                    if (!result.contains(category)) {
                        result.add(category);
                    }
                }
            }
        }
        return Collections.unmodifiableList(result);
    }

    /**
     * Overridden to return the categories in the subplots.
     *
     * @param axis ignored.
     * @return A list of the categories in the subplots.
     * @since 1.0.3
     */
    public List getCategoriesForAxis(CategoryAxis axis) {
        // FIXME:  this code means that it is not possible to use more than
        // one domain axis for the combined plots...
        return getCategories();
    }

    /**
     * Handles a 'click' on the plot.
     *
     * @param x    x-coordinate of the click.
     * @param y    y-coordinate of the click.
     * @param info information about the plot's dimensions.
     */
    public void handleClick(int x, int y, PlotRenderingInfo info) {

        Rectangle2D dataArea = info.getDataArea();
        if (dataArea.contains(x, y)) {
            for (int i = 0; i < this.subplots.size(); i++) {
                CategoryPlot subplot = subplots.get(i);
                PlotRenderingInfo subplotInfo = info.getSubplotInfo(i);
                subplot.handleClick(x, y, subplotInfo);
            }
        }

    }

    /**
     * Receives a {@link PlotChangeEvent} and responds by notifying all
     * listeners.
     *
     * @param event the event.
     */
    public void plotChanged(PlotChangeEvent event) {
        notifyListeners(event);
    }

    /**
     * Tests the plot for equality with an arbitrary object.
     *
     * @param obj the object (<code>null</code> permitted).
     * @return A boolean.
     */
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof CombinedDomainCategoryPlot)) {
            return false;
        }
        CombinedCategoryPlot that = (CombinedCategoryPlot) obj;
        if (this.gap != that.gap) {
            return false;
        }
        if (!ObjectUtilities.equal(this.subplots, that.subplots)) {
            return false;
        }
        return super.equals(obj);
    }

    /**
     * Returns a clone of the plot.
     *
     * @return A clone.
     * @throws CloneNotSupportedException this class will not throw this
     *                                    exception, but subclasses (if any) might.
     */
    public Object clone() throws CloneNotSupportedException {

        CombinedCategoryPlot result = (CombinedCategoryPlot) super.clone();
        result.subplots = (List) ObjectUtilities.deepClone(this.subplots);
        for (Iterator it = result.subplots.iterator(); it.hasNext();) {
            Plot child = (Plot) it.next();
            child.setParent(result);
        }
        return result;

    }

}

