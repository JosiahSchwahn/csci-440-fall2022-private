package edu.montana.csci.csci440.model;

import java.math.BigDecimal;
import edu.montana.csci.csci440.util.DB;
import edu.montana.csci.csci440.util.Web;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
public class InvoiceItem extends Model {

    Long invoiceLineId;
    Long invoiceId;
    Long trackId;
    BigDecimal unitPrice;
    Long quantity;

    String trackName;
    String albumName;
    String artistName;


    private InvoiceItem(ResultSet results) throws SQLException {
        invoiceLineId = results.getLong("InvoiceLineId");
        invoiceId = results.getLong("InvoiceId");
        trackId = results.getLong("TrackId");
        unitPrice = results.getBigDecimal("UnitPrice");
        quantity = results.getLong("Quantity");
        trackName = results.getString("TrackName");
        albumName = results.getString("AlbumName");
        artistName = results.getString("ArtistName");
    }

    public Track getTrack() {
        return null;
    }
    public Invoice getInvoice() {
        return null;
    }

    public Long getInvoiceLineId() {
        return invoiceLineId;
    }

    public void setInvoiceLineId(Long invoiceLineId) {
        this.invoiceLineId = invoiceLineId;
    }

    public Long getInvoiceId() {
        return invoiceId;
    }

    public void setInvoiceId(Long invoiceId) {
        this.invoiceId = invoiceId;
    }

    public Long getTrackId() {
        return trackId;
    }

    public void setTrackId(Long trackId) {
        this.trackId = trackId;
    }

    public BigDecimal getUnitPrice() {
        return unitPrice;
    }

    public void setUnitPrice(BigDecimal unitPrice) {
        this.unitPrice = unitPrice;
    }

    public Long getQuantity() {
        return quantity;
    }

    public void setQuantity(Long quantity) {
        this.quantity = quantity;
    }


    public static List<InvoiceItem> getForInvoice(long invoiceId) {
        try (Connection conn = DB.connect();
             PreparedStatement stmt = conn.prepareStatement(
                     "SELECT invoice_items.InvoiceLineId AS InvoiceLineId, invoice_items.InvoiceId AS InvoiceId, invoice_items.TrackId AS trackID, " +
                             "invoice_items.UnitPrice AS UnitPrice, invoice_items.Quantity AS Quantity, tracks.Name AS TrackName, albums.Title AS AlbumName, " +
                             "artists.Name AS ArtistName FROM invoice_items " +
                             "JOIN tracks ON invoice_items.TrackId = tracks.TrackId JOIN albums ON tracks.AlbumId = albums.AlbumId " +
                             "JOIN artists ON albums.ArtistId = artists.ArtistId WHERE InvoiceId=? LIMIT ? OFFSET ?")) {
            stmt.setLong(1, invoiceId);
            stmt.setInt(2, Web.PAGE_SIZE);
            stmt.setInt(3, (Web.getPage() - 1) * Web.PAGE_SIZE);
            ResultSet results = stmt.executeQuery();
            List<InvoiceItem> resultList = new LinkedList<>();
            while (results.next()) {
                resultList.add(new InvoiceItem(results));
            }
            return resultList;
        } catch (SQLException sqlException) {
            throw new RuntimeException(sqlException);
        }
    }



}
