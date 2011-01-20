namespace ise { namespace exceptions {

class Generic
{
public:
    Generic( const std::string& message ) : m_message(message)
    {
    }
    
    const std::string& str() const { return m_message; }
    
private:
    std::string m_message;
};

}}


#define ISE_THROW( e ) throw e
