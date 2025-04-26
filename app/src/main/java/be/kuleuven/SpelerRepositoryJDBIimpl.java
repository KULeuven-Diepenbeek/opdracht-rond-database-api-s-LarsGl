package be.kuleuven;

import java.sql.Connection;
import java.util.List;

import org.jdbi.v3.core.Jdbi;

public class SpelerRepositoryJDBIimpl implements SpelerRepository {
  private Jdbi jdbi;

  // Constructor
  SpelerRepositoryJDBIimpl(Connection connection) {
    jdbi = Jdbi.create(connection);
  }
  public Jdbi getJdbi() {
    return jdbi;
  }


  @Override
  public void addSpelerToDb(Speler speler) {
    jdbi.useHandle(handle -> {
      handle.createUpdate("INSERT INTO speler (tennisvlaanderenid, naam, punten) VALUES (:id, :naam, :punten)")
      .bind("id", speler.getTennisvlaanderenId())
      .bind("naam", speler.getNaam())
      .bind("punten", speler.getPunten())
      .execute();
    });
  }
  
  

  @Override
  public Speler getSpelerByTennisvlaanderenId(int tennisvlaanderenId) {
    Speler speler = jdbi.withHandle(handle -> {
        return handle.createQuery("SELECT * FROM speler where tennisvlaanderenid= :nummer")
            .bind("nummer", tennisvlaanderenId)
            .mapToBean(Speler.class)
            .findFirst()
            .orElse(null);
    });
  
    if (speler == null) {
      throw new InvalidSpelerException("Invalid Speler met identification: " + tennisvlaanderenId);
    }
  
    return speler;
  }
  

  @Override
  public List<Speler> getAllSpelers() {
    return jdbi.withHandle(handle -> {
      return handle.createQuery("SELECT * FROM speler")
          .mapToBean(Speler.class)
          .list();
    });
  }

  @Override
  public void updateSpelerInDb(Speler speler) {
    int affectedRows = jdbi.withHandle(handle -> {
      return handle
          .createUpdate(
              "UPDATE speler SET naam = :naam, punten = :punten WHERE tennisvlaanderenId = :tennisvlaanderenId;")
          .bindBean(speler)
          .execute();
    });
    if (affectedRows == 0) {
      throw new InvalidSpelerException(speler.getTennisvlaanderenId() + "");
    }
  }

  @Override
  public void deleteSpelerInDb(int tennisvlaanderenid) {
    int affectedRows = jdbi.withHandle(handle -> {
      return handle
          .createUpdate(
              "DELETE FROM speler WHERE tennisvlaanderenid = :tennisvlaanderenid")
          .bind("tennisvlaanderenid", tennisvlaanderenid)
          .execute();
    });
    if (affectedRows == 0) {
      throw new InvalidSpelerException(tennisvlaanderenid + "");
    }
  }


  @Override
  public String getHoogsteRankingVanSpeler(int tennisvlaanderenid) {
      // Speler ophalen via JDBI
      Speler found_speler = jdbi.withHandle(handle -> {
          return handle.createQuery("SELECT * FROM speler WHERE tennisvlaanderenid = :id")
              .bind("id", tennisvlaanderenid)
              .map((rs, ctx) -> new Speler(
                  rs.getInt("tennisvlaanderenid"),
                  rs.getString("naam"),
                  rs.getInt("punten")
              ))
              .findFirst()
              .orElse(null);
      });
  
      if (found_speler == null) {
          throw new InvalidSpelerException(tennisvlaanderenid + "");
      }
  
      // Hoogste ranking ophalen via JDBI
      String hoogsteRanking = jdbi.withHandle(handle -> {
          return handle.createQuery(
              "SELECT t.clubnaam, w.finale, w.winnaar " +
              "FROM wedstrijd w " +
              "JOIN tornooi t ON w.tornooi = t.id " +
              "WHERE (w.speler1 = :id OR w.speler2 = :id) AND w.finale IS NOT NULL " +
              "ORDER BY w.finale " +
              "LIMIT 1")
              .bind("id", tennisvlaanderenid)
              .map((rs, ctx) -> {
                  String tornooinaam = rs.getString("clubnaam");
                  int finale = rs.getInt("finale");
                  int winnaar = rs.getInt("winnaar");
  
                  String finaleString;
                  switch (finale) {
                      case 1:
                          finaleString = (winnaar == tennisvlaanderenid) ? "winst" : "finale";
                          break;
                      case 2:
                          finaleString = "halve finale";
                          break;
                      case 4:
                          finaleString = "kwart finale";
                          break;
                      default:
                          finaleString = "plaats " + finale;
                          break;
                  }
  
                  return "Hoogst geplaatst in het tornooi van " + tornooinaam +
                         " met plaats in de " + finaleString;
              })
              .first();
      });
  
      return hoogsteRanking;
  }
  

  @Override
  public void addSpelerToTornooi(int tornooiId, int tennisvlaanderenId) {
      jdbi.useHandle(handle -> {
          handle.createUpdate("INSERT INTO speler_speelt_tornooi (speler, tornooi) VALUES (:speler, :tornooi)")
                .bind("speler", tennisvlaanderenId)
                .bind("tornooi", tornooiId)
                .execute();
          handle.commit();
      });
  }
  

  @Override
  public void removeSpelerFromTornooi(int tornooiId, int tennisvlaanderenId) {
      jdbi.useHandle(handle ->
          handle.createUpdate("DELETE FROM speler_speelt_tornooi WHERE speler = :speler AND tornooi = :tornooi")
                .bind("speler", tennisvlaanderenId)
                .bind("tornooi", tornooiId)
                .execute()
      );
  }
}