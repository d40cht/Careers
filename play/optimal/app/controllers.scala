package controllers

import play._
import play.mvc._

object Application extends Controller {
    
    import views.Application._
    
    def index = html.index( session, flash )
    def login =
    {
        val name = params.get("username")

        if ( name == null )
        {
            html.login( session, flash )
        }        
        else if ( name == "alex" )
        {
            flash += ("info" -> ("Welcome " + name))
            session += ("user" -> "alex")
            Action(index)
        }
        else
        {
            flash += ("error" -> ("Unknown user " + name))
            html.login(session, flash)
        }
    }
    def logout =
    {
        val name = session.get("user")
        session.remove("user")
        flash += ("info" -> ("Goodbye: " + name) )
        Action(index)
    }
    
    def register = html.register( session, flash )
       
}
