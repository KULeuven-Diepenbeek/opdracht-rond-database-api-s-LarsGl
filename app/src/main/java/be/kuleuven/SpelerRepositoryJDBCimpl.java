package be.kuleuven;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;



public class SpelerRepositoryJDBCimpl implements SpelerRepository {
  private Connection connection;

  // Constructor
  SpelerRepositoryJDBCimpl(Connection connection) {
    this.connection = connection;
  }

  @Override
  public void addSpelerToDb(Speler speler) {
    try {
      PreparedStatement prepared = (PreparedStatement) connection
          .prepareStatement("INSERT INTO speler (tennisvlaanderenId, naam, punten) VALUES (?, ?, ?);");
          prepared.setInt(1, speler.getTennisvlaanderenId()); // First questionmark
          prepared.setString(2, speler.getNaam()); // Second questionmark
          prepared.setInt(3, speler.getPunten()); // Third questionmark
          prepared.executeUpdate();
          prepared.close();
          connection.commit();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Speler getSpelerByTennisvlaanderenId(int tennisvlaanderenId) {
    Speler found_speler = null;
    try {
      Statement s = (Statement) connection.createStatement();
      String stmt = "SELECT * FROM speler WHERE tennisvlaanderenid = '" + tennisvlaanderenId + "'";
      ResultSet result = s.executeQuery(stmt);

      while (result.next()) {
        int tennisvlaanderenid = result.getInt("tennisvlaanderenid");
        String naam = result.getString("naam");
        int punten = result.getInt("punten");

        found_speler = new Speler(tennisvlaanderenid, naam, punten);
      }
      // Check if speler is found
      // If not, throw an exception
      if (found_speler == null) {
        throw new InvalidSpelerException(tennisvlaanderenId + "");
      }

      result.close();
      s.close();
      connection.commit();
    } catch (Exception e) {
      throw new RuntimeException(e);//error
    }
    return found_speler;
  }

  @Override
  public List<Speler> getAllSpelers() {
    ArrayList<Speler> resultList = new ArrayList<Speler>();//lege lijst
    try {
      Statement s = (Statement) connection.createStatement();
      String stmt = "SELECT * FROM speler";
      ResultSet result = s.executeQuery(stmt);

      while (result.next()) {
        int tennisvlaanderenId = result.getInt("tennisvlaanderenid");
        String naam = result.getString("naam");
        int punten = result.getInt("punten");
        resultList.add(new Speler(tennisvlaanderenId, naam, punten));
      }

      result.close();
      s.close();
      connection.commit();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return resultList;
  }

  @Override
  public void updateSpelerInDb(Speler speler) {
    // Check if speler is already in DB
    getSpelerByTennisvlaanderenId(speler.getTennisvlaanderenId());
    try {
      // WITH prepared statement
      PreparedStatement prepared = (PreparedStatement) connection
          .prepareStatement("UPDATE speler SET naam = ?, punten = ? WHERE tennisvlaanderenId = ?;");
      prepared.setInt(3, speler.getTennisvlaanderenId()); // Third questionmark
      prepared.setString(1, speler.getNaam()); // First questionmark
      prepared.setInt(2, speler.getPunten()); // Second questionmark
      prepared.executeUpdate();
      prepared.close();
      connection.commit();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }


  @Override
  public void deleteSpelerInDb(int tennisvlaanderenid) {
    // Check if speler is already in DB
    getSpelerByTennisvlaanderenId(tennisvlaanderenid);
    try {
      // WITH prepared statement
      PreparedStatement prepared = (PreparedStatement) connection
          .prepareStatement("DELETE FROM speler WHERE tennisvlaanderenId = ?");
      prepared.setInt(1, tennisvlaanderenid); // First questionmark
      prepared.executeUpdate();

      prepared.close();
      connection.commit();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
@Override
public String getHoogsteRankingVanSpeler(int tennisvlaanderenid) {
    Speler found_speler = null;
    try (Statement s = connection.createStatement()) {
        String stmt = "SELECT * FROM speler WHERE tennisvlaanderenId = '" + tennisvlaanderenid + "'";
        try (ResultSet result = s.executeQuery(stmt)) {
            while (result.next()) {
                int tennisvlaanderenId = result.getInt("tennisvlaanderenid");
                String naam = result.getString("naam");
                int punten = result.getInt("punten");
                found_speler = new Speler(tennisvlaanderenid, naam, punten);
            }
        }
        // Check if speler is found
        // If not, throw an exception
        if (found_speler == null) {
            throw new InvalidSpelerException(tennisvlaanderenid + "");
        }
        connection.commit();
    } catch (SQLException e) {
        throw new RuntimeException("Error fetching player data", e);
    }
    //hoogste ranking
    String hoogsteRanking = null;
    try (PreparedStatement prepared = connection.prepareStatement(
        "SELECT t.clubnaam, w.finale, w.winnaar " +
        "FROM wedstrijd w " +
        "JOIN tornooi t ON w.tornooi = t.id " +
        "WHERE (w.speler1 = ? OR w.speler2 = ?) AND w.finale IS NOT NULL " +
        "ORDER BY w.finale " +
        "LIMIT 1;"
    )) {
        prepared.setInt(1, tennisvlaanderenid);
        prepared.setInt(2, tennisvlaanderenid);
        try (ResultSet result = prepared.executeQuery()) {
            if (result.next()) {
                String tornooinaam = result.getString("clubnaam");
                int finale = result.getInt("finale");
                int winnaar = result.getInt("winnaar");

                // Mapping finale to rounds:
                String finaleString;
                switch (finale) {
                    case 1:
                        if (winnaar == tennisvlaanderenid) {
                            finaleString = "winst";  // Player won the final
                        } else {
                            finaleString = "finale";  // Player lost in the final
                        }
                        break;
                    case 2:
                        finaleString = "halve finale";  // Semi-final round
                        break;
                    case 4:
                        finaleString = "kwart finale";  // Quarter-final round
                        break;
                    default:
                        finaleString = "plaats " + finale;  // For other rounds (e.g., 8, 16, etc.)
                        break;
                }

                hoogsteRanking = "Hoogst geplaatst in het tornooi van " + tornooinaam + " met plaats in de " + finaleString;
            } else {
                hoogsteRanking = "Geen ranking gevonden voor speler met ID " + tennisvlaanderenid;
            }
        }
        connection.commit();
    } catch (SQLException e) {
        throw new RuntimeException("Error fetching highest ranking", e);
    }

    return hoogsteRanking;
}




  @Override
  public void addSpelerToTornooi(int tornooiId, int tennisvlaanderenId){
    //check player exists
    getSpelerByTennisvlaanderenId(tennisvlaanderenId);
    try {
      // WITH prepared statement
      PreparedStatement prepared = (PreparedStatement) connection
          .prepareStatement("INSERT INTO speler_speelt_tornooi (speler, tornooi) VALUES (?, ?);");
      prepared.setInt(1, tennisvlaanderenId); // First questionmark
      prepared.setInt(2, tornooiId); // Second questionmark
      prepared.executeUpdate();

      prepared.close();
      connection.commit();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  

  @Override
  public void removeSpelerFromTornooi(int tornooiId, int tennisvlaanderenId){
    //check player exists
    getSpelerByTennisvlaanderenId(tennisvlaanderenId);
    try {
      // WITH prepared statement
      PreparedStatement prepared = (PreparedStatement) connection
          .prepareStatement("DELETE FROM speler_speelt_tornooi WHERE speler = ? AND tornooi = ?;");
      prepared.setInt(1, tennisvlaanderenId); // First questionmark
      prepared.setInt(2, tornooiId); // Second questionmark
      prepared.executeUpdate();

      prepared.close();
      connection.commit();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
