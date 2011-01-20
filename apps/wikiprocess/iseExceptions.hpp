namespace ise { namespace exceptions {

class Generic
{
public:
    Generic( const std::string& message )
    {
    }
};

}}


#define ISE_THROW( e ) throw e
